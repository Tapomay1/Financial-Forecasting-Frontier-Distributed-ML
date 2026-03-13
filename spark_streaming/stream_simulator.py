"""
STREAM SIMULATOR
Splits bank.csv into small chunks and drops them into /data/stream/
to simulate real-time transactions for spark_streaming.py.

Run in a separate terminal:
  docker exec spark-master python /app/spark_streaming/stream_simulator.py
"""

import pandas as pd
import time
import os
from datetime import datetime

INPUT_FILE = "/data/bank.csv"
OUTPUT_DIR = "/data/stream"
CHUNK_SIZE = 50       # rows per file
DELAY_SECS = 5        # seconds between chunks

os.makedirs(OUTPUT_DIR, exist_ok=True)

df = pd.read_csv(INPUT_FILE)
df["event_time"] = None  # will be filled with current time per chunk

total_chunks = (len(df) + CHUNK_SIZE - 1) // CHUNK_SIZE
print(f"Dataset: {len(df)} rows → {total_chunks} chunks of {CHUNK_SIZE} rows")
print(f"Output directory: {OUTPUT_DIR}")
print(f"Sending one chunk every {DELAY_SECS} seconds ...\n")

for i in range(0, len(df), CHUNK_SIZE):
    chunk = df.iloc[i:i + CHUNK_SIZE].copy()
    chunk["event_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    fname = os.path.join(OUTPUT_DIR, f"chunk_{i:06d}.csv")
    chunk.to_csv(fname, index=False)
    chunk_num = i // CHUNK_SIZE + 1
    print(f"Sent chunk {chunk_num}/{total_chunks} → {fname}")
    time.sleep(DELAY_SECS)

print("\nAll chunks sent. Stream simulation complete.")
