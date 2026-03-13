#!/usr/bin/env python3
"""
LOCAL MAPREDUCE RUNNER
Simulates Hadoop Streaming locally for testing all 5 MapReduce jobs.
No Hadoop required – run this directly:
  python3 mapreduce/mr_runner.py
"""

import csv, sys
from collections import defaultdict

DATA_FILE = "data/bank.csv"

def load_rows():
    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        return list(reader)

rows = load_rows()

# ──────────────────────────────────────────────────────────────────────────────
# MR-1: Average balance per job
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("MR-1: Average Account Balance per Job Type")
print("="*60)

totals = defaultdict(lambda: [0.0, 0])
for row in rows:
    job, balance = row[1].strip(), row[5].strip()
    try:
        totals[job][0] += float(balance)
        totals[job][1] += 1
    except ValueError:
        pass

print(f"{'Job':<20} {'Avg Balance':>15} {'Count':>8}")
print("-" * 46)
for job, (total, count) in sorted(totals.items(), key=lambda x: -x[1][0]/x[1][1]):
    avg = total / count
    print(f"{job:<20} {avg:>15.2f} {count:>8}")

# ──────────────────────────────────────────────────────────────────────────────
# MR-2: Housing loan count per education
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("MR-2: Housing Loan Count per Education Category")
print("="*60)

counts = defaultdict(int)
for row in rows:
    education, housing = row[3].strip(), row[6].strip()
    counts[(education, housing)] += 1

print(f"{'Education':<15} {'Housing Loan':<15} {'Count':>8}")
print("-" * 40)
for (edu, housing), count in sorted(counts.items()):
    print(f"{edu:<15} {housing:<15} {count:>8}")

# ──────────────────────────────────────────────────────────────────────────────
# MR-3: Contacts per month + subscription status
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("MR-3: Contacts per Month by Subscription Status")
print("="*60)

month_counts = defaultdict(lambda: defaultdict(int))
for row in rows:
    month, y = row[10].strip(), row[16].strip()
    month_counts[month][y] += 1

MONTH_ORDER = {m: i for i, m in enumerate(
    ["jan","feb","mar","apr","may","jun",
     "jul","aug","sep","oct","nov","dec"])}

print(f"{'Month':<8} {'yes':>8} {'no':>8} {'total':>8}")
print("-" * 36)
for month in sorted(month_counts, key=lambda m: MONTH_ORDER.get(m, 99)):
    yes = month_counts[month]["yes"]
    no  = month_counts[month]["no"]
    print(f"{month:<8} {yes:>8} {no:>8} {yes+no:>8}")

# ──────────────────────────────────────────────────────────────────────────────
# MR-4: Average contact duration per poutcome
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("MR-4: Average Contact Duration per Previous Campaign Outcome")
print("="*60)

dur_totals = defaultdict(lambda: [0.0, 0])
for row in rows:
    poutcome, duration = row[15].strip(), row[11].strip()
    try:
        dur_totals[poutcome][0] += float(duration)
        dur_totals[poutcome][1] += 1
    except ValueError:
        pass

print(f"{'poutcome':<15} {'Avg Duration (s)':>18} {'Count':>8}")
print("-" * 44)
for poutcome, (total, count) in sorted(dur_totals.items()):
    avg = total / count if count else 0
    print(f"{poutcome:<15} {avg:>18.2f} {count:>8}")

# ──────────────────────────────────────────────────────────────────────────────
# MR-5: Age vs Balance relationship
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("MR-5: Age vs Balance Relationship (by Decade)")
print("="*60)

age_data = defaultdict(lambda: [0.0, 0, float("inf"), float("-inf")])
for row in rows:
    age, balance = row[0].strip(), row[5].strip()
    try:
        band = f"{(int(age)//10)*10}s"
        bal  = float(balance)
        age_data[band][0] += bal
        age_data[band][1] += 1
        age_data[band][2]  = min(age_data[band][2], bal)
        age_data[band][3]  = max(age_data[band][3], bal)
    except ValueError:
        pass

print(f"{'Age Band':<10} {'Avg Balance':>14} {'Count':>8} {'Min':>10} {'Max':>10}")
print("-" * 56)
for band, (total, count, mn, mx) in sorted(age_data.items()):
    avg = total / count if count else 0
    print(f"{band:<10} {avg:>14.2f} {count:>8} {mn:>10.0f} {mx:>10.0f}")

print("\n=== All MapReduce jobs complete ===")
