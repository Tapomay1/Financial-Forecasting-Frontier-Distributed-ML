#!/usr/bin/env python3
"""
MAPREDUCE PROGRAMS – Banking Dataset
Pure Python MapReduce (Hadoop Streaming compatible).

Run via Hadoop Streaming inside namenode container:
  docker exec namenode hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input  /user/hadoop/banking/bank.csv \
    -output /user/hadoop/output/avg_balance_by_job \
    -mapper  "python3 /tmp/mr_avg_balance_mapper.py" \
    -reducer "python3 /tmp/mr_avg_balance_reducer.py"

OR run locally (for testing):
  python3 mapreduce/mr_runner.py
"""

# ──────────────────────────────────────────────────────────────────────────────
# MR-1 MAPPER: Average account balance per job type
# ──────────────────────────────────────────────────────────────────────────────
MR1_MAPPER = '''#!/usr/bin/env python3
import sys, csv
reader = csv.reader(sys.stdin)
next(reader)  # skip header
for row in reader:
    if len(row) < 17:
        continue
    job, balance = row[1].strip(), row[5].strip()
    try:
        print(f"{job}\\t{float(balance)}")
    except ValueError:
        pass
'''

# ──────────────────────────────────────────────────────────────────────────────
# MR-1 REDUCER: Average account balance per job type
# ──────────────────────────────────────────────────────────────────────────────
MR1_REDUCER = '''#!/usr/bin/env python3
import sys
from collections import defaultdict
totals = defaultdict(lambda: [0.0, 0])
for line in sys.stdin:
    parts = line.strip().split("\\t")
    if len(parts) != 2:
        continue
    job, balance = parts[0], float(parts[1])
    totals[job][0] += balance
    totals[job][1] += 1
for job, (total, count) in sorted(totals.items()):
    avg = total / count if count else 0
    print(f"{job}\\t{avg:.2f}")
'''

# ──────────────────────────────────────────────────────────────────────────────
# MR-2 MAPPER: Housing loan count per education category
# ──────────────────────────────────────────────────────────────────────────────
MR2_MAPPER = '''#!/usr/bin/env python3
import sys, csv
reader = csv.reader(sys.stdin)
next(reader)
for row in reader:
    if len(row) < 17:
        continue
    education, housing = row[3].strip(), row[6].strip()
    key = f"{education}\\t{housing}"
    print(f"{key}\\t1")
'''

# ──────────────────────────────────────────────────────────────────────────────
# MR-2 REDUCER: Housing loan count per education category
# ──────────────────────────────────────────────────────────────────────────────
MR2_REDUCER = '''#!/usr/bin/env python3
import sys
from collections import defaultdict
counts = defaultdict(int)
for line in sys.stdin:
    parts = line.strip().split("\\t")
    if len(parts) != 3:
        continue
    education, housing, _ = parts
    counts[(education, housing)] += 1
for (education, housing), count in sorted(counts.items()):
    print(f"{education}\\t{housing}\\t{count}")
'''

# ──────────────────────────────────────────────────────────────────────────────
# MR-3 MAPPER: Contacts per month + subscription status
# ──────────────────────────────────────────────────────────────────────────────
MR3_MAPPER = '''#!/usr/bin/env python3
import sys, csv
reader = csv.reader(sys.stdin)
next(reader)
for row in reader:
    if len(row) < 17:
        continue
    month, y = row[10].strip(), row[16].strip()
    key = f"{month}\\t{y}"
    print(f"{key}\\t1")
'''

# ──────────────────────────────────────────────────────────────────────────────
# MR-3 REDUCER: Contacts per month + subscription status
# ──────────────────────────────────────────────────────────────────────────────
MR3_REDUCER = '''#!/usr/bin/env python3
import sys
from collections import defaultdict
counts = defaultdict(int)
for line in sys.stdin:
    parts = line.strip().split("\\t")
    if len(parts) != 3:
        continue
    month, y, _ = parts
    counts[(month, y)] += 1
MONTH_ORDER = {m: i for i, m in enumerate(
    ["jan","feb","mar","apr","may","jun",
     "jul","aug","sep","oct","nov","dec"])}
for (month, y), count in sorted(counts.items(),
        key=lambda x: MONTH_ORDER.get(x[0][0], 99)):
    print(f"{month}\\t{y}\\t{count}")
'''

# ──────────────────────────────────────────────────────────────────────────────
# MR-4 MAPPER: Average contact duration per poutcome
# ──────────────────────────────────────────────────────────────────────────────
MR4_MAPPER = '''#!/usr/bin/env python3
import sys, csv
reader = csv.reader(sys.stdin)
next(reader)
for row in reader:
    if len(row) < 17:
        continue
    poutcome, duration = row[15].strip(), row[11].strip()
    try:
        print(f"{poutcome}\\t{float(duration)}")
    except ValueError:
        pass
'''

# ──────────────────────────────────────────────────────────────────────────────
# MR-4 REDUCER: Average contact duration per poutcome
# ──────────────────────────────────────────────────────────────────────────────
MR4_REDUCER = '''#!/usr/bin/env python3
import sys
from collections import defaultdict
totals = defaultdict(lambda: [0.0, 0])
for line in sys.stdin:
    parts = line.strip().split("\\t")
    if len(parts) != 2:
        continue
    poutcome, dur = parts[0], float(parts[1])
    totals[poutcome][0] += dur
    totals[poutcome][1] += 1
for poutcome, (total, count) in sorted(totals.items()):
    avg = total / count if count else 0
    print(f"{poutcome}\\t{avg:.2f}")
'''

# ──────────────────────────────────────────────────────────────────────────────
# MR-5 MAPPER: Age vs balance relationship
# ──────────────────────────────────────────────────────────────────────────────
MR5_MAPPER = '''#!/usr/bin/env python3
import sys, csv
reader = csv.reader(sys.stdin)
next(reader)
for row in reader:
    if len(row) < 17:
        continue
    age, balance = row[0].strip(), row[5].strip()
    try:
        age_band = (int(age) // 10) * 10
        print(f"{age_band}s\\t{float(balance)}")
    except ValueError:
        pass
'''

# ──────────────────────────────────────────────────────────────────────────────
# MR-5 REDUCER: Age vs balance relationship
# ──────────────────────────────────────────────────────────────────────────────
MR5_REDUCER = '''#!/usr/bin/env python3
import sys
from collections import defaultdict
data = defaultdict(lambda: [0.0, 0, float("inf"), float("-inf")])
for line in sys.stdin:
    parts = line.strip().split("\\t")
    if len(parts) != 2:
        continue
    age_band, balance = parts[0], float(parts[1])
    data[age_band][0] += balance
    data[age_band][1] += 1
    data[age_band][2] = min(data[age_band][2], balance)
    data[age_band][3] = max(data[age_band][3], balance)
print("age_band\\tavg_balance\\tcount\\tmin_balance\\tmax_balance")
for age_band, (total, count, mn, mx) in sorted(data.items()):
    avg = total / count if count else 0
    print(f"{age_band}\\t{avg:.2f}\\t{count}\\t{mn:.0f}\\t{mx:.0f}")
'''


# ─── Write all MR files to disk ───────────────────────────────────────────────
import os, stat
scripts = {
    "mr1_mapper.py":  MR1_MAPPER,
    "mr1_reducer.py": MR1_REDUCER,
    "mr2_mapper.py":  MR2_MAPPER,
    "mr2_reducer.py": MR2_REDUCER,
    "mr3_mapper.py":  MR3_MAPPER,
    "mr3_reducer.py": MR3_REDUCER,
    "mr4_mapper.py":  MR4_MAPPER,
    "mr4_reducer.py": MR4_REDUCER,
    "mr5_mapper.py":  MR5_MAPPER,
    "mr5_reducer.py": MR5_REDUCER,
}

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
for fname, code in scripts.items():
    path = os.path.join(OUT_DIR, fname)
    with open(path, "w") as f:
        f.write(code)
    os.chmod(path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

print("All MapReduce scripts written to:", OUT_DIR)
