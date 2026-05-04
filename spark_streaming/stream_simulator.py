#!/usr/bin/env python3
"""
STREAM SIMULATOR (Refactored)

Simulates real-time data ingestion by splitting a dataset into chunks
and writing them incrementally to a directory.

Usage:
    python stream_simulator.py
"""

import os
import time
from datetime import datetime
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
INPUT_FILE = "/data/bank.csv"
OUTPUT_DIR = "/data/stream"

CHUNK_SIZE = 50     # Rows per batch
DELAY_SECS = 5      # Delay between batches (seconds)


# ─────────────────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────────────────
def ensure_output_dir(path):
    """Ensure output directory exists."""
    os.makedirs(path, exist_ok=True)


def load_dataset(path):
    """Load dataset into pandas DataFrame."""
    return pd.read_csv(path)


# ─────────────────────────────────────────────────────────────────────────────
# CORE LOGIC
# ─────────────────────────────────────────────────────────────────────────────
def simulate_stream(df):
    """Send dataset in chunks to simulate streaming."""

    total_rows = len(df)
    total_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE

    print(f"Dataset size      : {total_rows} rows")
    print(f"Chunk size        : {CHUNK_SIZE}")
    print(f"Total chunks      : {total_chunks}")
    print(f"Output directory  : {OUTPUT_DIR}")
    print(f"Delay per chunk   : {DELAY_SECS} seconds\n")

    for idx in range(0, total_rows, CHUNK_SIZE):
        chunk_id = idx // CHUNK_SIZE + 1

        # Create chunk
        chunk = df.iloc[idx:idx + CHUNK_SIZE].copy()

        # Add event timestamp
        chunk["event_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Write chunk to file
        filename = os.path.join(OUTPUT_DIR, f"chunk_{idx:06d}.csv")
        chunk.to_csv(filename, index=False)

        print(f"[{chunk_id}/{total_chunks}] Sent → {filename}")

        time.sleep(DELAY_SECS)

    print("\n=== Stream simulation completed ===")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXECUTION
# ─────────────────────────────────────────────────────────────────────────────
def main():
    ensure_output_dir(OUTPUT_DIR)

    df = load_dataset(INPUT_FILE)

    # Initialize event_time column
    df["event_time"] = None

    simulate_stream(df)


if __name__ == "__main__":
    main()