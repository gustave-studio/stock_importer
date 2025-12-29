import sys
from datetime import datetime

import pandas as pd
import yfinance as yf
import boto3

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import DecimalType, LongType

# ========= 引数 =========
# Glue の引数で --DATE だけ渡す想定
args = getResolvedOptions(sys.argv, ['DATE'])
acq_date = args['DATE']          # 例: "20251105"

bucket_name = "gs-analysis-platform"
target = "AAPL"

print("=== Glue stock_price_importer start ===", flush=True)
print(f"[DEBUG] acq_date(DATE) = {acq_date}", flush=True)
print(f"[DEBUG] target        = {target}", flush=True)
print(f"[DEBUG] bucket        = {bucket_name}", flush=True)
print(f"[DEBUG] current UTC   = {datetime.utcnow().isoformat()}", flush=True)

# ========= Glue / Spark コンテキスト =========
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 圧縮形式を SNAPPY に
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# ========= yfinance から「直近1営業日分」を取得 =========
# ※ テーブルの date / acq_date は引数 DATE を使うので、
#    yfinance 上の日付はこの時点では特に使わない
print("=== [DEBUG] Call yfinance.download(period='1d') ===", flush=True)

data = None
try:
    # ここは「動いていた」形にできるだけ寄せる
    data = yf.download(
        target,
        period="1d",
        interval="1d",
        group_by="column",
    )
    print("[DEBUG] Raw data from yfinance:", flush=True)
    print(data, flush=True)
    print(f"[DEBUG] type(data)   = {type(data)}", flush=True)
    if data is not None:
        print(f"[DEBUG] data.empty   = {data.empty}", flush=True)
        print(f"[DEBUG] data.index   = {data.index}", flush=True)
        print(f"[DEBUG] data.columns = {getattr(data, 'columns', None)}", flush=True)
except Exception as e:
    print(f"[ERROR] yfinance.download で例外発生: {repr(e)}", flush=True)
    data = None

if data is None or data.empty:
    print("[WARN] yfinance がデータを返しませんでした。", flush=True)
    print("      （未確定・休場日・ネットワークなどの可能性）", flush=True)
    # 今回は「エラー」扱いにして気付きやすくする
    raise Exception("yfinance が空データを返しました")

# ========= MultiIndex / カラム名の正規化 =========
print("=== [DEBUG] Normalize pandas DataFrame columns ===", flush=True)

if isinstance(data.columns, pd.MultiIndex):
    print("[DEBUG] data.columns is MultiIndex", flush=True)
    print(f"[DEBUG] raw MultiIndex columns = {list(data.columns)}", flush=True)

    level0 = [c[0] for c in data.columns]
    level1 = [c[1] for c in data.columns]

    # パターン1: 全部 ('Price', 'Close') みたいに level0 が同じ
    # → level1 を使う: ['Close','High','Low','Open','Volume']
    if len(set(level0)) == 1:
        use_cols = level1
        print(f"[DEBUG] use level1 as columns = {use_cols}", flush=True)
    else:
        # パターン2: ('Open','AAPL'), ('High','AAPL') ... みたいな形
        # → level0 を使う: ['Open','High','Low','Close','Adj Close','Volume']
        use_cols = level0
        print(f"[DEBUG] use level0 as columns = {use_cols}", flush=True)

    data.columns = use_cols

else:
    print("[DEBUG] data.columns is Index", flush=True)
    print(f"[DEBUG] columns = {data.columns.tolist()}", flush=True)

# Date index を通常カラムに戻す（ただし後で捨てる）
data = data.reset_index()
print("[DEBUG] After reset_index()", flush=True)
print(data.head(), flush=True)
print(f"[DEBUG] columns(after reset) = {data.columns.tolist()}", flush=True)

# 全カラムを小文字 + スペース→アンダースコアに
rename_map = {col: str(col).lower().replace(" ", "_") for col in data.columns}
data = data.rename(columns=rename_map)

print("[DEBUG] After rename to lowercase/underscore:", flush=True)
print(data.head(), flush=True)
print(f"[DEBUG] columns(after rename) = {data.columns.tolist()}", flush=True)

# adj_close が無い場合は close をコピー
if "adj_close" not in data.columns and "close" in data.columns:
    print("[DEBUG] adj_close が無いため close をコピーします", flush=True)
    data["adj_close"] = data["close"]

# pandas 側で必要なカラムだけ残しておく
needed_cols = ["open", "high", "low", "close", "adj_close", "volume"]
missing = [c for c in needed_cols if c not in data.columns]
if missing:
    print(f"[ERROR] 期待する列が足りません: {missing}", flush=True)
    print(f"[ERROR] 現在の columns = {data.columns.tolist()}", flush=True)
    raise Exception(f"Missing expected columns: {missing}")

data = data[needed_cols]

print("[DEBUG] pandas DataFrame ready for Spark:", flush=True)
print(data, flush=True)

# ========= pandas → Spark DataFrame =========
spark_df = spark.createDataFrame(data)

# DATE 引数を date / acq_date に入れ、import_datetime を追加
spark_df = (
    spark_df
    .withColumn("date", lit(acq_date))              # string
    .withColumn("import_datetime", current_timestamp())  # timestamp
    .withColumn("acq_date", lit(acq_date))         # partition 用
)

# カラム順をテーブルと合わせる（acq_date は最後でOK）
spark_df = spark_df.select(
    "date", "open", "high", "low", "close", "adj_close", "volume", "import_datetime", "acq_date"
)

# ========= 型キャスト（decimal / bigint / timestamp） =========
dec_type = DecimalType(38, 12)

for col_name in ["open", "high", "low", "close", "adj_close"]:
    spark_df = spark_df.withColumn(col_name, spark_df[col_name].cast(dec_type))

spark_df = spark_df.withColumn("volume", spark_df["volume"].cast(LongType()))
# import_datetime は current_timestamp() で既に timestamp 型のはず

print("=== [DEBUG] Final Spark DataFrame ===", flush=True)
spark_df.show(truncate=False)
spark_df.printSchema()

# ========= Parquet を S3 に出力 =========
# Hive 形式のパーティション: .../data/stock_price/acq_date=YYYYMMDD/...
output_root = "s3://gs-analysis-platform/data/stock_price/"

print(f"[DEBUG] Write Parquet to {output_root} (partitioned by acq_date)", flush=True)

(
    spark_df
    .repartition(1)  # 1ファイルにしたければ（任意）
    .write
    .mode("overwrite")   # 同じ acq_date を上書きする運用なら
    .partitionBy("acq_date")
    .parquet(output_root)
)

print("[INFO] Parquet write finished.", flush=True)
print("=== Glue stock_price_importer end ===", flush=True)
