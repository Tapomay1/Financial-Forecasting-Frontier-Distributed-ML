"""
Hadoop Streaming MapReduce Scripts Generator
Refactored Version: Clean structure, reusable helpers, improved readability
"""

import os
import stat


# ─────────────────────────────────────────────────────────────────────────────
# COMMON TEMPLATE HELPERS
# ─────────────────────────────────────────────────────────────────────────────
COMMON_IMPORTS = """#!/usr/bin/env python3
import sys
import csv
"""

REDUCER_IMPORTS = """#!/usr/bin/env python3
import sys
from collections import defaultdict
"""


# ─────────────────────────────────────────────────────────────────────────────
# MR-1: Average Balance per Job
# ─────────────────────────────────────────────────────────────────────────────
MR1_MAPPER = COMMON_IMPORTS + """
reader = csv.reader(sys.stdin)
next(reader)

for row in reader:
    if len(row) < 17:
        continue

    job = row[1].strip()
    balance = row[5].strip()

    try:
        print(f"{job}\\t{float(balance)}")
    except ValueError:
        continue
"""

MR1_REDUCER = REDUCER_IMPORTS + """
totals = defaultdict(lambda: [0.0, 0])

for line in sys.stdin:
    key, value = line.strip().split("\\t")
    value = float(value)

    totals[key][0] += value
    totals[key][1] += 1

for job, (total, count) in sorted(totals.items()):
    avg = total / count if count else 0
    print(f"{job}\\t{avg:.2f}")
"""


# ─────────────────────────────────────────────────────────────────────────────
# MR-2: Housing Loan Count by Education
# ─────────────────────────────────────────────────────────────────────────────
MR2_MAPPER = COMMON_IMPORTS + """
reader = csv.reader(sys.stdin)
next(reader)

for row in reader:
    if len(row) < 17:
        continue

    education = row[3].strip()
    housing = row[6].strip()

    print(f"{education}\\t{housing}\\t1")
"""

MR2_REDUCER = REDUCER_IMPORTS + """
counts = defaultdict(int)

for line in sys.stdin:
    education, housing, _ = line.strip().split("\\t")
    counts[(education, housing)] += 1

for (education, housing), count in sorted(counts.items()):
    print(f"{education}\\t{housing}\\t{count}")
"""


# ─────────────────────────────────────────────────────────────────────────────
# MR-3: Monthly Contacts + Subscription Status
# ─────────────────────────────────────────────────────────────────────────────
MR3_MAPPER = COMMON_IMPORTS + """
reader = csv.reader(sys.stdin)
next(reader)

for row in reader:
    if len(row) < 17:
        continue

    month = row[10].strip()
    status = row[16].strip()

    print(f"{month}\\t{status}\\t1")
"""

MR3_REDUCER = REDUCER_IMPORTS + """
counts = defaultdict(int)

MONTH_ORDER = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12
}

for line in sys.stdin:
    month, status, _ = line.strip().split("\\t")
    counts[(month, status)] += 1

for (month, status), count in sorted(
    counts.items(),
    key=lambda x: MONTH_ORDER.get(x[0][0], 99)
):
    print(f"{month}\\t{status}\\t{count}")
"""


# ─────────────────────────────────────────────────────────────────────────────
# MR-4: Average Duration by Campaign Outcome
# ─────────────────────────────────────────────────────────────────────────────
MR4_MAPPER = COMMON_IMPORTS + """
reader = csv.reader(sys.stdin)
next(reader)

for row in reader:
    if len(row) < 17:
        continue

    outcome = row[15].strip()
    duration = row[11].strip()

    try:
        print(f"{outcome}\\t{float(duration)}")
    except ValueError:
        continue
"""

MR4_REDUCER = REDUCER_IMPORTS + """
totals = defaultdict(lambda: [0.0, 0])

for line in sys.stdin:
    key, value = line.strip().split("\\t")
    value = float(value)

    totals[key][0] += value
    totals[key][1] += 1

for outcome, (total, count) in sorted(totals.items()):
    avg = total / count if count else 0
    print(f"{outcome}\\t{avg:.2f}")
"""


# ─────────────────────────────────────────────────────────────────────────────
# MR-5: Age Band vs Balance Analysis
# ─────────────────────────────────────────────────────────────────────────────
MR5_MAPPER = COMMON_IMPORTS + """
reader = csv.reader(sys.stdin)
next(reader)

for row in reader:
    if len(row) < 17:
        continue

    age = row[0].strip()
    balance = row[5].strip()

    try:
        age_band = (int(age) // 10) * 10
        print(f"{age_band}s\\t{float(balance)}")
    except ValueError:
        continue
"""

MR5_REDUCER = REDUCER_IMPORTS + """
stats = defaultdict(lambda: [0.0, 0, float("inf"), float("-inf")])

for line in sys.stdin:
    age_band, value = line.strip().split("\\t")
    value = float(value)

    stats[age_band][0] += value
    stats[age_band][1] += 1
    stats[age_band][2] = min(stats[age_band][2], value)
    stats[age_band][3] = max(stats[age_band][3], value)

print("age_band\\tavg_balance\\tcount\\tmin_balance\\tmax_balance")

for age_band, (total, count, mn, mx) in sorted(stats.items()):
    avg = total / count if count else 0
    print(f"{age_band}\\t{avg:.2f}\\t{count}\\t{mn:.0f}\\t{mx:.0f}")
"""


# ─────────────────────────────────────────────────────────────────────────────
# SCRIPT WRITER
# ─────────────────────────────────────────────────────────────────────────────
def write_scripts(output_dir):
    """Write all mapper and reducer scripts to disk."""
    scripts = {
        "mr1_mapper.py": MR1_MAPPER,
        "mr1_reducer.py": MR1_REDUCER,
        "mr2_mapper.py": MR2_MAPPER,
        "mr2_reducer.py": MR2_REDUCER,
        "mr3_mapper.py": MR3_MAPPER,
        "mr3_reducer.py": MR3_REDUCER,
        "mr4_mapper.py": MR4_MAPPER,
        "mr4_reducer.py": MR4_REDUCER,
        "mr5_mapper.py": MR5_MAPPER,
        "mr5_reducer.py": MR5_REDUCER,
    }

    for filename, content in scripts.items():
        file_path = os.path.join(output_dir, filename)

        with open(file_path, "w") as f:
            f.write(content)

        # Make scripts executable
        os.chmod(file_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

    print(f"All MapReduce scripts generated in: {output_dir}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXECUTION
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    write_scripts(base_dir)