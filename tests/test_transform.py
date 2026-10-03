import pytest
from poc.spark import get_spark
from poc.transform import dedupe_latest, transform_df

@pytest.fixture(scope="session")
def spark():
    return get_spark()

def test_latest_version_wins(spark):
    df = spark.createDataFrame(
        [("P1", 10.0, "2026-10-01 10:00:00", "Y"),
         ("P1", 20.0, "2026-10-02 10:00:00", "Y")],
        ["pymt_id", "pymt_amt", "updt_ts", "FUND_EXTN_ACCT_IN"])
    out = dedupe_latest(df).collect()
    assert len(out) == 1 and float(out[0]["pymt_amt"]) == 20.0

def test_payment_type_and_audit_columns(spark):
    df = spark.createDataFrame(
        [("P1", 1.0, "2026-10-01 10:00:00", "Y"),
         ("P2", 1.0, "2026-10-01 10:00:00", "N"),
         ("P3", 1.0, "2026-10-01 10:00:00", None)],
        ["pymt_id", "pymt_amt", "updt_ts", "FUND_EXTN_ACCT_IN"])
    out = {r["pymt_id"]: r for r in transform_df(df, "2026-10-04").collect()}
    assert out["P1"]["pymt_type_cd"] == 6
    assert out["P2"]["pymt_type_cd"] == 8
    assert out["P3"]["pymt_type_cd"] is None
    assert "extract_transform_load_timestamp" in out["P1"].asDict()
    assert str(out["P1"]["effective_date"]) == "2026-10-04"
