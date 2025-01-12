import yfinance as yf
import sys
import boto3
import os
import pandas as pd
from awsglue.utils import getResolvedOptions

# コマンドライン引数を解析する
args = getResolvedOptions(sys.argv, ['DATE'])

# S3にアップロードするための情報
bucket_name = "f-test-glue00"

# 銘柄を指定
target = "AAPL"

# yfinanceからデータを取得
data = yf.download(target, period='1d', interval='1d')

print("Raw Data from yfinance:")
print(data)

# データのヘッダーを確認して動的に設定
data.index.name = 'Date'

# 列の定義
expected_cols_5 = ["Open", "High", "Low", "Close", "Volume"]
expected_cols_6 = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

if data.shape[1] == 5:  # 列数が5の場合
    data.columns = expected_cols_5
elif data.shape[1] == 6:  # 列数が6の場合
    data.columns = expected_cols_6
else:
    print(f"予期しない列数: {data.shape[1]} (columns: {data.columns})")
    sys.exit(1)  # 不明な形式の場合に終了

print("Processed Data with Renamed Columns:")
print(data)

# 引数から日付を取得 (例: '20241005')
date_str = args['DATE']

# 出力ファイルパス（Parquet形式）
parquet_file_path = f"/tmp/stock_price_{date_str}.parquet"
output_s3_file_name = f"se2/out0/stock_price/{date_str}/stock_price.parquet"

# Parquetファイルとして保存
data.to_parquet(parquet_file_path, index=True, engine="pyarrow")

# S3にファイルをアップロード
s3 = boto3.client('s3')

try:
    # ParquetファイルをS3にアップロード
    s3.upload_file(parquet_file_path, bucket_name, output_s3_file_name)
    print(f"ParquetファイルがS3バケット '{bucket_name}' のパス '{output_s3_file_name}' に正常にアップロードされました。")
except Exception as e:
    print(f"ファイルのアップロード中にエラーが発生しました: {e}")

# 一時ファイルの削除（必要に応じて）
if os.path.exists(parquet_file_path):
    os.remove(parquet_file_path)
