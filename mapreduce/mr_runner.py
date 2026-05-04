#!/usr/bin/env python3
"""
LOCAL MAPREDUCE RUNNER (Refactored)

Simulates Hadoop Streaming jobs locally using Python.
Executes all 5 MapReduce-style analyses on banking dataset.

Run:
    python3 mapreduce/mr_runner.py
"""

import csv
from collections import defaultdict


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
DATA_FILE = "data/bank.csv"


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
def load_dataset(path):
    """Load CSV data into memory (skip header)."""
    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        return list(reader)


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
def print_section(title):
    """Pretty print section header."""
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# MR-1: Average Balance per Job
# ─────────────────────────────────────────────────────────────────────────────
def mr1_avg_balance(rows):
    print_section("MR-1: Average Account Balance per Job Type")

    stats = defaultdict(lambda: [0.0, 0])

    for row in rows:
        try:
            job = row[1].strip()
            balance = float(row[5])
            stats[job][0] += balance
            stats[job][1] += 1
        except ValueError:
            continue

    print(f"{'Job':<20} {'Avg Balance':>15} {'Count':>8}")
    print("-" * 46)

    for job, (total, count) in sorted(
        stats.items(),
        key=lambda x: -(x[1][0] / x[1][1])
    ):
        avg = total / count if count else 0
        print(f"{job:<20} {avg:>15.2f} {count:>8}")


# ─────────────────────────────────────────────────────────────────────────────
# MR-2: Housing Loan Count per Education
# ─────────────────────────────────────────────────────────────────────────────
def mr2_housing_by_education(rows):
    print_section("MR-2: Housing Loan Count per Education")

    counts = defaultdict(int)

    for row in rows:
        education = row[3].strip()
        housing = row[6].strip()
        counts[(education, housing)] += 1

    print(f"{'Education':<15} {'Housing Loan':<15} {'Count':>8}")
    print("-" * 40)

    for (edu, housing), count in sorted(counts.items()):
        print(f"{edu:<15} {housing:<15} {count:>8}")


# ─────────────────────────────────────────────────────────────────────────────
# MR-3: Monthly Contacts + Subscription Status
# ─────────────────────────────────────────────────────────────────────────────
def mr3_contacts_by_month(rows):
    print_section("MR-3: Contacts per Month by Subscription Status")

    data = defaultdict(lambda: defaultdict(int))

    for row in rows:
        month = row[10].strip()
        status = row[16].strip()
        data[month][status] += 1

    month_order = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "may": 5, "jun": 6, "jul": 7, "aug": 8,
        "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }

    print(f"{'Month':<8} {'Yes':>8} {'No':>8} {'Total':>8}")
    print("-" * 36)

    for month in sorted(data.keys(), key=lambda m: month_order.get(m, 99)):
        yes = data[month]["yes"]
        no = data[month]["no"]
        total = yes + no

        print(f"{month:<8} {yes:>8} {no:>8} {total:>8}")


# ─────────────────────────────────────────────────────────────────────────────
# MR-4: Avg Duration per Campaign Outcome
# ─────────────────────────────────────────────────────────────────────────────
def mr4_duration_by_outcome(rows):
    print_section("MR-4: Average Contact Duration per Campaign Outcome")

    stats = defaultdict(lambda: [0.0, 0])

    for row in rows:
        try:
            outcome = row[15].strip()
            duration = float(row[11])
            stats[outcome][0] += duration
            stats[outcome][1] += 1
        except ValueError:
            continue

    print(f"{'Outcome':<15} {'Avg Duration (s)':>18} {'Count':>8}")
    print("-" * 44)

    for outcome, (total, count) in sorted(stats.items()):
        avg = total / count if count else 0
        print(f"{outcome:<15} {avg:>18.2f} {count:>8}")


# ─────────────────────────────────────────────────────────────────────────────
# MR-5: Age vs Balance Analysis
# ─────────────────────────────────────────────────────────────────────────────
def mr5_age_balance(rows):
    print_section("MR-5: Age vs Balance Relationship (Decade-wise)")

    stats = defaultdict(lambda: [0.0, 0, float("inf"), float("-inf")])

    for row in rows:
        try:
            age = int(row[0])
            balance = float(row[5])

            band = f"{(age // 10) * 10}s"

            stats[band][0] += balance
            stats[band][1] += 1
            stats[band][2] = min(stats[band][2], balance)
            stats[band][3] = max(stats[band][3], balance)

        except ValueError:
            continue

    print(f"{'Age Band':<10} {'Avg Balance':>14} {'Count':>8} {'Min':>10} {'Max':>10}")
    print("-" * 56)

    for band, (total, count, mn, mx) in sorted(stats.items()):
        avg = total / count if count else 0
        print(f"{band:<10} {avg:>14.2f} {count:>8} {mn:>10.0f} {mx:>10.0f}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXECUTION
# ─────────────────────────────────────────────────────────────────────────────
def main():
    rows = load_dataset(DATA_FILE)

    mr1_avg_balance(rows)
    mr2_housing_by_education(rows)
    mr3_contacts_by_month(rows)
    mr4_duration_by_outcome(rows)
    mr5_age_balance(rows)

    print("\n=== All MapReduce jobs completed successfully ===")


if __name__ == "__main__":
    main()