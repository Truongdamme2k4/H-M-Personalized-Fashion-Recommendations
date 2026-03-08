import pandas as pd
import numpy as np
import os
import json
import logging
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

VALID_CLUB_STATUS = {"ACTIVE", "PRE-CREATE", "LEFT CLUB"}
VALID_NEWS_FREQ   = {"Regularly", "Monthly", "None"}
AGE_MIN, AGE_MAX  = 15, 100

CLUB_STATUS_MAP = {"ACTIVE": 0, "PRE-CREATE": 1, "LEFT CLUB": 2, "UNKNOWN": -1}
NEWS_FREQ_MAP   = {"None": 0, "Monthly": 1, "Regularly": 2}


def _check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    n_dup = df.duplicated(subset=["customer_id"]).sum()
    if n_dup:
        log.warning(f"Phát hiện {n_dup:,} customer_id trùng lặp → Xóa.")
        df = df.drop_duplicates(subset=["customer_id"], keep="first")
    return df


def _validate(df: pd.DataFrame) -> None:
    assert df.isnull().sum().sum() == 0, "Vẫn còn giá trị NaN sau xử lý!"
    assert df["age"].between(AGE_MIN, AGE_MAX).all(), "Tuổi có giá trị ngoài bounds!"
    assert df["club_member_status"].isin([-1, 0, 1, 2]).all(), "club_member_status có giá trị lạ!"
    assert df["fashion_news_frequency"].isin([0, 1, 2]).all(), "fashion_news_frequency có giá trị lạ!"
    assert df["FN"].isin([0, 1]).all(), "FN có giá trị ngoài {0, 1}!"
    assert df["Active"].isin([0, 1]).all(), "Active có giá trị ngoài {0, 1}!"
    log.info("Validation passed.")


def process_customers(
    input_path: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> Optional[pd.DataFrame]:

    # 1. ĐƯỜNG DẪN
    current_dir = os.path.dirname(os.path.abspath(__file__))

    if input_path is None:
        input_path = os.path.abspath(os.path.join(current_dir, "../../data/raw/customers.csv"))
    if output_dir is None:
        output_dir = os.path.abspath(os.path.join(current_dir, "../../data/processed"))

    if not os.path.exists(input_path):
        log.error(f"Không tìm thấy file tại: {input_path}")
        return None

    # 2. ĐỌC FILE
    df = pd.read_csv(input_path, dtype={"customer_id": str})
    initial_mem = df.memory_usage(deep=True).sum() / 1024**2
    log.info(f"Đọc xong: {df.shape[0]:,} dòng × {df.shape[1]} cột | RAM: {initial_mem:.2f} MB")

    # 3. KIỂM TRA CỘT BẮT BUỘC
    required_cols = {"customer_id", "FN", "Active", "club_member_status",
                     "fashion_news_frequency", "age", "postal_code"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        log.error(f"Thiếu cột bắt buộc: {missing_cols}")
        return None

    # 4. DUPLICATE & VALIDATE customer_id
    df = _check_duplicates(df)
    df = df[df["customer_id"].notna()].copy()
    df["customer_id"] = df["customer_id"].str.strip()
    df = df[df["customer_id"] != ""].copy()

    # 5. BINARY: FN, Active
    #    Tạo flag missing trước — 65% null mang thông tin hành vi
    for col in ["FN", "Active"]:
        df[f"{col}_missing"] = df[col].isnull().astype(np.int8)
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[~df[col].isin([0.0, 1.0]), col] = np.nan
        df[col] = df[col].fillna(0).astype(np.int8)

    # 6. TUỔI — outlier → NaN, điền median theo nhóm club_member_status
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df.loc[(df["age"] < AGE_MIN) | (df["age"] > AGE_MAX), "age"] = np.nan

    df["age"] = (
        df.groupby("club_member_status")["age"]
        .transform(lambda x: x.fillna(x.median()))
    )
    # Fallback: nếu cả nhóm đều null thì điền median toàn bộ
    global_median_age = df["age"].median()
    df["age"] = df["age"].fillna(global_median_age).astype(np.int8)

    # 7. club_member_status → Label Encoding
    #    ACTIVE=0, PRE-CREATE=1, LEFT CLUB=2, UNKNOWN=-1
    df["club_member_status"] = (
        df["club_member_status"]
        .astype(str).str.strip().str.upper()
        .replace({"NAN": "UNKNOWN", "": "UNKNOWN"})
    )
    df.loc[
        ~df["club_member_status"].isin(VALID_CLUB_STATUS | {"UNKNOWN"}),
        "club_member_status"
    ] = "UNKNOWN"
    df["club_member_status"] = df["club_member_status"].map(CLUB_STATUS_MAP).astype(np.int8)

    # 8. fashion_news_frequency → Label Encoding
    #    None=0, Monthly=1, Regularly=2
    df["fashion_news_frequency"] = (
        df["fashion_news_frequency"]
        .astype(str).str.strip()
        .replace({"NONE": "None", "none": "None", "NO": "None", "nan": "None", "": "None"})
    )
    df.loc[
        ~df["fashion_news_frequency"].isin(VALID_NEWS_FREQ),
        "fashion_news_frequency"
    ] = "None"
    df["fashion_news_frequency"] = df["fashion_news_frequency"].map(NEWS_FREQ_MAP).astype(np.int8)

    # 9. XÓA postal_code (hash ẩn danh, không có giá trị cho model)
    df.drop(columns=["postal_code"], inplace=True)

    # 10. VALIDATION
    _validate(df)

    # 11. LƯU FILE + METADATA
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "customers_cleaned.parquet")
    df.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")

    meta = {
        "club_member_status":     CLUB_STATUS_MAP,
        "fashion_news_frequency": NEWS_FREQ_MAP,
        "age_bounds":             [AGE_MIN, AGE_MAX],
        "global_median_age":      global_median_age,
        "columns":                list(df.columns),
        "n_rows":                 len(df),
    }
    meta_path = os.path.join(output_dir, "customers_meta.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    final_mem = df.memory_usage(deep=True).sum() / 1024**2
    log.info(
        f"Xong: {len(df):,} dòng | "
        f"RAM: {initial_mem:.2f} MB → {final_mem:.2f} MB "
        f"(↓{((initial_mem - final_mem) / initial_mem) * 100:.1f}%)"
    )
    log.info(f"💾 Data : {output_path}")
    log.info(f"📋 Meta : {meta_path}")

    print(df.head().to_string())

    return df


if __name__ == "__main__":
    customers_cleaned = process_customers()