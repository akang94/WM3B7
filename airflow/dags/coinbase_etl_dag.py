"""
Coinbase ETL Pipeline DAG
WM3B7 - Data Science and Machine Learning Assignment 1

Pipeline:
t1_extract -> t2_clean -> t3_join -> t4_transform -> t5_load -> t6_visualise
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

# ── Paths ────────────────────────────────────────────────
DATA_DIR      = '/workspaces/WM3B7/data'
TICKER_FILE   = f'{DATA_DIR}/ticker_data.csv'
MATCHES_FILE  = f'{DATA_DIR}/matches_data.csv'
STAGING_FILE  = f'{DATA_DIR}/staging_combined.csv'
FINAL_FILE    = f'{DATA_DIR}/final_dataset.csv'
DB_PATH       = f'{DATA_DIR}/coinbase.db'
VIZ_DIR       = '/workspaces/WM3B7/visualisations'

# ── DAG default arguments ────────────────────────────────
default_args = {
    'owner': 'WM3B7_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ── Task 1: Extract ──────────────────────────────────────
def extract_data(**kwargs):
    """Extract data from both CSV source files."""
    print("=== T1: EXTRACTING DATA ===")

    ticker_df  = pd.read_csv(TICKER_FILE)
    matches_df = pd.read_csv(MATCHES_FILE)

    print(f"Ticker rows:  {len(ticker_df)}")
    print(f"Matches rows: {len(matches_df)}")
    print(f"Ticker columns:  {list(ticker_df.columns)}")
    print(f"Matches columns: {list(matches_df.columns)}")

    # Push to XCom so next tasks can use the data
    kwargs['ti'].xcom_push(key='ticker_rows',  value=len(ticker_df))
    kwargs['ti'].xcom_push(key='matches_rows', value=len(matches_df))
    print("✅ Extraction complete")

# ── Task 2: Clean ────────────────────────────────────────
def clean_data(**kwargs):
    """Clean and validate both datasets."""
    print("=== T2: CLEANING DATA ===")

    ticker_df  = pd.read_csv(TICKER_FILE)
    matches_df = pd.read_csv(MATCHES_FILE)

    # --- Clean ticker ---
    print(f"Ticker nulls before: {ticker_df.isnull().sum().sum()}")
    ticker_df.drop_duplicates(inplace=True)
    ticker_df.dropna(subset=['price', 'product_id', 'time'], inplace=True)

    # Fix data types
    ticker_df['price']      = pd.to_numeric(ticker_df['price'],      errors='coerce')
    ticker_df['open_24h']   = pd.to_numeric(ticker_df['open_24h'],   errors='coerce')
    ticker_df['volume_24h'] = pd.to_numeric(ticker_df['volume_24h'], errors='coerce')
    ticker_df['low_24h']    = pd.to_numeric(ticker_df['low_24h'],    errors='coerce')
    ticker_df['high_24h']   = pd.to_numeric(ticker_df['high_24h'],   errors='coerce')
    ticker_df['best_bid']   = pd.to_numeric(ticker_df['best_bid'],   errors='coerce')
    ticker_df['best_ask']   = pd.to_numeric(ticker_df['best_ask'],   errors='coerce')
    ticker_df['time']       = pd.to_datetime(ticker_df['time'],      errors='coerce')
    ticker_df.dropna(subset=['price'], inplace=True)

    # --- Clean matches ---
    print(f"Matches nulls before: {matches_df.isnull().sum().sum()}")
    matches_df.drop_duplicates(subset=['trade_id'], inplace=True)
    matches_df.dropna(subset=['price', 'size', 'product_id'], inplace=True)

    matches_df['price']    = pd.to_numeric(matches_df['price'], errors='coerce')
    matches_df['size']     = pd.to_numeric(matches_df['size'],  errors='coerce')
    matches_df['time']     = pd.to_datetime(matches_df['time'], errors='coerce')
    matches_df.dropna(subset=['price', 'size'], inplace=True)

    # Save cleaned files
    ticker_df.to_csv(f'{DATA_DIR}/ticker_clean.csv',   index=False)
    matches_df.to_csv(f'{DATA_DIR}/matches_clean.csv', index=False)

    print(f"Ticker rows after cleaning:  {len(ticker_df)}")
    print(f"Matches rows after cleaning: {len(matches_df)}")
    print("✅ Cleaning complete")

# ── Task 3: Join ─────────────────────────────────────────
def join_data(**kwargs):
    """Join ticker and matches into one consolidated dataset."""
    print("=== T3: JOINING DATA ===")

    ticker_df  = pd.read_csv(f'{DATA_DIR}/ticker_clean.csv')
    matches_df = pd.read_csv(f'{DATA_DIR}/matches_clean.csv')

    # Parse timestamps
    ticker_df['time']  = pd.to_datetime(ticker_df['time'])
    matches_df['time'] = pd.to_datetime(matches_df['time'])

    # Round timestamps to nearest second for joining
    ticker_df['time_rounded']  = ticker_df['time'].dt.round('1s')
    matches_df['time_rounded'] = matches_df['time'].dt.round('1s')

    # Merge on product_id and rounded time
    combined_df = pd.merge(
        matches_df,
        ticker_df[['product_id', 'time_rounded', 'best_bid',
                   'best_ask', 'open_24h', 'volume_24h',
                   'low_24h', 'high_24h']],
        on=['product_id', 'time_rounded'],
        how='left'
    )

    combined_df.to_csv(STAGING_FILE, index=False)
    print(f"Combined dataset rows: {len(combined_df)}")
    print(f"Columns: {list(combined_df.columns)}")
    print("✅ Join complete")

# ── Task 4: Transform ────────────────────────────────────
def transform_data(**kwargs):
    """Apply transformations and feature engineering."""
    print("=== T4: TRANSFORMING DATA ===")

    df = pd.read_csv(STAGING_FILE)
    df['time'] = pd.to_datetime(df['time'])

    # Calculate bid-ask spread
    df['spread'] = df['best_ask'] - df['best_bid']

    # Calculate trade value
    df['trade_value'] = df['price'] * df['size']

    # Calculate price position within 24h range
    df['price_range_position'] = (
        (df['price'] - df['low_24h']) /
        (df['high_24h'] - df['low_24h'])
    ).round(4)

    # Label buy/sell
    df['side'] = df['side'].str.upper()

    # Calculate VWAP per product
    for product in df['product_id'].unique():
        mask = df['product_id'] == product
        subset = df[mask]
        vwap = (subset['price'] * subset['size']).sum() / subset['size'].sum()
        df.loc[mask, 'vwap'] = round(vwap, 2)
        print(f"VWAP for {product}: {round(vwap, 2)}")

    # Extract hour for time-based analysis
    df['hour'] = df['time'].dt.hour
    df['minute'] = df['time'].dt.minute

    df.to_csv(FINAL_FILE, index=False)
    print(f"Final dataset rows: {len(df)}")
    print(f"Final columns: {list(df.columns)}")
    print("✅ Transform complete")

# ── Task 5: Load ─────────────────────────────────────────
def load_data(**kwargs):
    """Load final dataset into SQLite database."""
    print("=== T5: LOADING TO DATABASE ===")

    df = pd.read_csv(FINAL_FILE)

    conn = sqlite3.connect(DB_PATH)

    # Write to database (replace if exists)
    df.to_sql('coinbase_trades', conn, if_exists='replace', index=False)

    # Verify load
    result = pd.read_sql('SELECT COUNT(*) as total FROM coinbase_trades', conn)
    print(f"Rows in database: {result['total'][0]}")

    # Show sample per product
    sample = pd.read_sql('''
        SELECT product_id, COUNT(*) as trades,
               ROUND(AVG(price), 2) as avg_price,
               ROUND(SUM(trade_value), 2) as total_value
        FROM coinbase_trades
        GROUP BY product_id
    ''', conn)
    print(sample.to_string())
    conn.close()
    print(f"✅ Data loaded to {DB_PATH}")

# ── Task 6: Visualise ────────────────────────────────────
def visualise_data(**kwargs):
    """Generate visualisations from the final dataset."""
    print("=== T6: GENERATING VISUALISATIONS ===")

    os.makedirs(VIZ_DIR, exist_ok=True)
    df = pd.read_csv(FINAL_FILE)
    df['time'] = pd.to_datetime(df['time'])

    sns.set_theme(style='darkgrid')
    colors = {'BTC-USD': '#F7931A', 'ETH-USD': '#627EEA', 'SOL-USD': '#9945FF'}

    # Chart 1 — Price over time per coin
    fig, ax = plt.subplots(figsize=(12, 5))
    for product in df['product_id'].unique():
        subset = df[df['product_id'] == product].sort_values('time')
        ax.plot(subset['time'], subset['price'],
                label=product, color=colors.get(product),
                linewidth=1.5)
    ax.set_title('Crypto Price Over Time', fontsize=14)
    ax.set_xlabel('Time')
    ax.set_ylabel('Price (USD)')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.tight_layout()
    plt.savefig(f'{VIZ_DIR}/price_over_time.png', dpi=150)
    plt.close()
    print("Saved price_over_time.png")

    # Chart 2 — Trade volume by coin (buy vs sell)
    fig, ax = plt.subplots(figsize=(10, 5))
    volume_df = df.groupby(['product_id', 'side'])['size'].sum().reset_index()
    sns.barplot(data=volume_df, x='product_id', y='size',
                hue='side', ax=ax,
                palette={'BUY': '#2ecc71', 'SELL': '#e74c3c'})
    ax.set_title('Trade Volume by Coin and Side', fontsize=14)
    ax.set_xlabel('Product')
    ax.set_ylabel('Total Volume')
    plt.tight_layout()
    plt.savefig(f'{VIZ_DIR}/volume_by_coin.png', dpi=150)
    plt.close()
    print("Saved volume_by_coin.png")

    # Chart 3 — Bid-ask spread over time
    fig, ax = plt.subplots(figsize=(12, 5))
    for product in df['product_id'].unique():
        subset = df[df['product_id'] == product].dropna(
                    subset=['spread']).sort_values('time')
        ax.plot(subset['time'], subset['spread'],
                label=product, color=colors.get(product),
                linewidth=1)
    ax.set_title('Bid-Ask Spread Over Time', fontsize=14)
    ax.set_xlabel('Time')
    ax.set_ylabel('Spread (USD)')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.tight_layout()
    plt.savefig(f'{VIZ_DIR}/spread_over_time.png', dpi=150)
    plt.close()
    print("Saved spread_over_time.png")

    print(f"✅ All visualisations saved to {VIZ_DIR}")

# ── Define DAG ───────────────────────────────────────────
with DAG(
    dag_id='coinbase_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Coinbase real-time crypto data',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['coinbase', 'etl', 'kafka', 'wm3b7'],
) as dag:

    t1_extract = PythonOperator(
        task_id='t1_extract',
        python_callable=extract_data,
    )

    t2_clean = PythonOperator(
        task_id='t2_clean',
        python_callable=clean_data,
    )

    t3_join = PythonOperator(
        task_id='t3_join',
        python_callable=join_data,
    )

    t4_transform = PythonOperator(
        task_id='t4_transform',
        python_callable=transform_data,
    )

    t5_load = PythonOperator(
        task_id='t5_load',
        python_callable=load_data,
    )

    t6_visualise = PythonOperator(
        task_id='t6_visualise',
        python_callable=visualise_data,
    )

    # ── Task pipeline ────────────────────────────────────
    t1_extract >> t2_clean >> t3_join >> t4_transform >> t5_load >> t6_visualise
