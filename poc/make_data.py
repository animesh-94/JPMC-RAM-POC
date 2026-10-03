import random
from datetime import datetime, timedelta
from pyspark.sql import Row

def make_rows(n_payments=1000, seed=42):
    random.seed(seed)
    base = datetime(2026, 10, 1, 8, 0, 0)
    rows = []
    for i in range(n_payments):
        versions = random.randint(1, 3)          # multiple versions of the same payment
        for v in range(versions):
            rows.append(Row(
                pymt_id=f"P{i:06d}",
                pymt_amt=round(random.uniform(1, 2000), 2),
                updt_ts=(base + timedelta(minutes=v * 10 + i)).strftime("%Y-%m-%d %H:%M:%S"),
                FUND_EXTN_ACCT_IN=random.choice(["Y", "N", None]),
            ))
    return rows
