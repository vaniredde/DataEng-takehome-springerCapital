#!/usr/bin/env python3
"""
your_script.py - Springer Capital Data Engineer Take-Home Test
Pandas implementation (patched for tz-aware vs tz-naive handling).

Outputs:
  - output/final_referral_report.csv
  - output/final_referral_report_46.csv  (expected 46 rows)
"""

import pandas as pd
import os

# -----------------------
# Paths - Windows-friendly
# -----------------------
BASE_DIR = os.path.join(os.getcwd(), "DE Dataset - Intern")  # your dataset folder
OUT_DIR = os.path.join(os.getcwd(), "output")
os.makedirs(OUT_DIR, exist_ok=True)


def load_csv(fname):
    return pd.read_csv(os.path.join(BASE_DIR, fname), dtype=str).replace({"": None})


# -----------------------
# Discover files
# -----------------------
files = os.listdir(BASE_DIR)


def pick(names):
    for n in names:
        if n in files:
            return n
    return None


lead_log_f = pick(["lead_log.csv", "lead_logs.csv"])
paid_tx_f = pick(["paid_transactions.csv", "paid_transaction.csv"])
ref_rewards_f = pick(["referral_rewards.csv"])
user_logs_f = pick(["user_logs.csv"])
user_referral_logs_f = pick(["user_referral_logs.csv"])
user_referral_statuses_f = pick(["user_referral_statuses.csv"])
user_referrals_f = pick(["user_referrals.csv"])

lead_log = load_csv(lead_log_f) if lead_log_f else pd.DataFrame()
paid_transactions = load_csv(paid_tx_f) if paid_tx_f else pd.DataFrame()
referral_rewards = load_csv(ref_rewards_f) if ref_rewards_f else pd.DataFrame()
user_logs = load_csv(user_logs_f) if user_logs_f else pd.DataFrame()
user_referral_logs = load_csv(user_referral_logs_f) if user_referral_logs_f else pd.DataFrame()
user_referral_statuses = load_csv(user_referral_statuses_f) if user_referral_statuses_f else pd.DataFrame()
user_referrals = load_csv(user_referrals_f) if user_referrals_f else pd.DataFrame()

# normalize column names
for df in [
    lead_log,
    paid_transactions,
    referral_rewards,
    user_logs,
    user_referral_logs,
    user_referral_statuses,
    user_referrals,
]:
    if not df.empty:
        df.columns = [c.strip() for c in df.columns]


# -----------------------
# Robust datetime helper
# -----------------------
def try_dt(s):
    return pd.to_datetime(s, errors="coerce")


def to_naive(series):
    """
    Convert a Series (or scalar-like) to timezone-naive datetimes.
    If values are timezone-aware, convert to UTC then drop tzinfo.
    Returns a pandas Series of dtype datetime64[ns] (naive).
    """
    s = pd.to_datetime(series, errors="coerce")

    # If dtype is timezone-aware (DatetimeTZDtype), convert to UTC then remove tz
    try:
        if hasattr(pd, "DatetimeTZDtype") and isinstance(getattr(s, "dtype", None), pd.DatetimeTZDtype):
            return s.dt.tz_convert("UTC").dt.tz_localize(None)
    except Exception:
        pass

    # If the series has a tz (elementwise), handle with elementwise conversion
    # (this handles mixed cases)
    try:
        if getattr(s.dt, "tz", None) is not None:
            return s.dt.tz_convert("UTC").dt.tz_localize(None)
    except Exception:
        pass

    # Fallback: already naive or coerced; return as-is
    return s


# -----------------------
# Pre-parse datetimes in original tables (keep originals where necessary)
# -----------------------
# Parse likely datetime columns first (they may have tz info)
for col in ["referral_at", "updated_at"]:
    if col in user_referrals.columns:
        user_referrals[col] = try_dt(user_referrals[col])

for col in ["created_at", "reward_granted_at"]:
    if col in user_referral_logs.columns:
        user_referral_logs[col] = try_dt(user_referral_logs[col])

if "transaction_at" in paid_transactions.columns:
    paid_transactions["transaction_at"] = try_dt(paid_transactions["transaction_at"])

if "created_at" in lead_log.columns:
    lead_log["created_at"] = try_dt(lead_log["created_at"])

if "membership_expired_date" in user_logs.columns:
    user_logs["membership_expired_date"] = try_dt(user_logs["membership_expired_date"])

# -----------------------
# Build base df and joins
# -----------------------
df = user_referrals.copy()

# Status join
if not user_referral_statuses.empty and "user_referral_status_id" in df.columns:
    status_map = user_referral_statuses.rename(
        columns={"id": "status_id", "description": "referral_status"}
    )[["status_id", "referral_status"]]
    df = df.merge(status_map, left_on="user_referral_status_id", right_on="status_id", how="left")

# Rewards join
if not referral_rewards.empty and "referral_reward_id" in df.columns:
    df = df.merge(referral_rewards.rename(columns={"id": "reward_id"}), left_on="referral_reward_id", right_on="reward_id", how="left")

# Referral logs join (prefix urlog_)
if not user_referral_logs.empty and "user_referral_id" in user_referral_logs.columns:
    if "referral_id" in df.columns:
        df = df.merge(user_referral_logs.add_prefix("urlog_"), left_on="referral_id", right_on="urlog_user_referral_id", how="left")
    else:
        df = df.merge(user_referral_logs.add_prefix("urlog_"), left_on="id", right_on="urlog_user_referral_id", how="left")

# Paid transactions join (prefix tx_)
if not paid_transactions.empty and "transaction_id" in df.columns and "transaction_id" in paid_transactions.columns:
    df = df.merge(paid_transactions.add_prefix("tx_"), left_on="transaction_id", right_on="tx_transaction_id", how="left")

# Lead log join (prefix lead_)
if not lead_log.empty and "referral_source" in df.columns and "lead_id" in lead_log.columns:
    df = df.merge(lead_log.add_prefix("lead_"), left_on="referee_id", right_on="lead_lead_id", how="left")

# User logs join for referrer info (prefix usr_)
if not user_logs.empty and "referrer_id" in df.columns and "user_id" in user_logs.columns:
    df = df.merge(user_logs.add_prefix("usr_"), left_on="referrer_id", right_on="usr_user_id", how="left")


# -----------------------
# Normalize and build 'out' columns (using tz-naive normalized datetimes)
# -----------------------
out = pd.DataFrame()
out["referral_details_id"] = df.get("id")
out["referral_id"] = df.get("referral_id")
out["referral_source"] = df.get("referral_source")


def src_cat(r):
    source = r.get("referral_source")
    if source == "User Sign Up":
        return "Online"
    if source == "Draft Transaction":
        return "Offline"
    if source == "Lead":
        # lead_source_category likely lives in lead_ prefixed columns
        return r.get("lead_source_category") or r.get("lead_source_category")
    return None


out["referral_source_category"] = df.apply(src_cat, axis=1)

# normalize datetimes to naive UTC-based datetimes
out["referral_at"] = to_naive(df.get("referral_at"))
out["updated_at"] = to_naive(df.get("updated_at"))
out["transaction_at"] = to_naive(df.get("tx_transaction_at", df.get("transaction_at")))
out["reward_granted_at"] = to_naive(df.get("urlog_reward_granted_at", df.get("reward_granted_at")))

out["referrer_id"] = df.get("referrer_id")
out["referrer_name"] = df.get("usr_name") if "usr_name" in df.columns else df.get("referrer_name")
out["referrer_phone_number"] = df.get("usr_phone_number") if "usr_phone_number" in df.columns else df.get("referrer_phone")
out["referrer_homeclub"] = df.get("usr_homeclub") if "usr_homeclub" in df.columns else df.get("referrer_homeclub")
out["referee_id"] = df.get("referee_id")
out["referee_name"] = df.get("referee_name")
out["referee_phone"] = df.get("referee_phone")
out["referral_status"] = df.get("referral_status")
out["num_reward_days"] = df.get("num_reward_days")
out["transaction_id"] = df.get("transaction_id")
out["transaction_status"] = df.get("tx_transaction_status") if "tx_transaction_status" in df.columns else df.get("transaction_status")
out["transaction_location"] = df.get("tx_transaction_location") if "tx_transaction_location" in df.columns else df.get("transaction_location")
out["transaction_type"] = df.get("tx_transaction_type") if "tx_transaction_type" in df.columns else df.get("transaction_type")
out["reward_value"] = df.get("reward_value")

# -----------------------
# Flags: make sure membership expiry is also tz-normalized
# -----------------------
if "usr_membership_expired_date" in df.columns:
    usr_expiry_naive = to_naive(df["usr_membership_expired_date"])
    # ensure both series have the same index / alignment; out["referral_at"] aligns with df index
    out["referrer_membership_not_expired"] = usr_expiry_naive > out["referral_at"]
else:
    out["referrer_membership_not_expired"] = True

if "usr_is_deleted" in df.columns:
    out["referrer_not_deleted"] = ~df["usr_is_deleted"].astype(str).str.lower().isin(["1", "true", "yes"])
else:
    out["referrer_not_deleted"] = True

if "urlog_is_reward_granted" in df.columns:
    out["is_reward_granted"] = df["urlog_is_reward_granted"].astype(str).str.lower().isin(["1", "true", "yes"])
elif "is_reward_granted" in df.columns:
    out["is_reward_granted"] = df["is_reward_granted"].astype(str).str.lower().isin(["1", "true", "yes"])
else:
    out["is_reward_granted"] = False


# -----------------------
# Business logic validation (robust)
# -----------------------
def safe_str(x):
    return "" if pd.isna(x) else str(x)


def compute_business_valid(row):
    try:
        reward_val = float(row.get("reward_value")) if row.get("reward_value") not in (None, "", "nan") else None
    except:
        reward_val = None

    status = safe_str(row.get("referral_status")).strip()
    tx_status = safe_str(row.get("transaction_status")).strip().upper()
    tx_type = safe_str(row.get("transaction_type")).strip().upper()
    tx_at = row.get("transaction_at")
    ref_at = row.get("referral_at")

    cond1 = False
    if reward_val is not None and reward_val > 0:
        if status == "Berhasil" and pd.notna(row.get("transaction_id")):
            if tx_status == "PAID" and tx_type == "NEW":
                if pd.notna(tx_at) and pd.notna(ref_at):
                    same_month = (tx_at.year == ref_at.year and tx_at.month == ref_at.month)
                    after_ref = tx_at >= ref_at
                    if same_month and after_ref and row.get("referrer_membership_not_expired") and row.get("referrer_not_deleted") and row.get("is_reward_granted"):
                        cond1 = True

    cond2 = False
    if status in ["Menunggu", "Tidak Berhasil"] and (reward_val in (None, 0)):
        cond2 = True

    # invalid checks
    invalid = False
    if reward_val is not None and reward_val > 0 and status != "Berhasil":
        invalid = True
    if reward_val is not None and reward_val > 0 and pd.isna(row.get("transaction_id")):
        invalid = True
    if (reward_val in (None, 0)) and pd.notna(row.get("transaction_id")) and tx_status == "PAID":
        invalid = True
    if status == "Berhasil" and (reward_val in (None, 0)):
        invalid = True
    if pd.notna(tx_at) and pd.notna(ref_at) and tx_at < ref_at:
        invalid = True

    if invalid:
        return False

    return bool(cond1 or cond2)


out["is_business_logic_valid"] = out.apply(compute_business_valid, axis=1)

# -----------------------
# Save full report
# -----------------------
final_cols = [
    "referral_details_id",
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at",
    "referrer_id",
    "referrer_name",
    "referrer_phone_number",
    "referrer_homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "referral_status",
    "num_reward_days",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "reward_granted_at",
    "is_business_logic_valid",
]

final = out.reindex(columns=[c for c in final_cols if c in out.columns])
final_full_path = os.path.join(OUT_DIR, "final_referral_report.csv")
final.to_csv(final_full_path, index=False)
print("Saved final report to:", final_full_path)

# -----------------------
# Aggregate to one row per referral_id (46 rows expected)
# -----------------------
if "referral_id" in final.columns:
    agg_dict = {}
    for c in final.columns:
        if c == "referral_id":
            continue
        if c in ["referral_at", "transaction_at", "updated_at", "reward_granted_at"]:
            agg_dict[c] = "max"
        elif c == "is_business_logic_valid":
            agg_dict[c] = "max"
        else:
            # first non-null value
            agg_dict[c] = lambda x: next((v for v in x if pd.notna(v)), None)

    final_agg = final.groupby("referral_id", as_index=False).agg(agg_dict)

    # ensure boolean column is boolean type
    if "is_business_logic_valid" in final_agg.columns:
        final_agg["is_business_logic_valid"] = final_agg["is_business_logic_valid"].astype(bool)

    agg_path = os.path.join(OUT_DIR, "final_referral_report_46.csv")
    final_agg.to_csv(agg_path, index=False)
    print("Saved aggregated 46-row final report to:", agg_path)
else:
    print("referral_id not found – skipping aggregation.")
