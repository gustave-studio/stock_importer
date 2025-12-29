import yfinance as yf
import sys
import csv
import boto3
import os
from awsglue.utils import getResolvedOptions
from datetime import datetime

# コマンドライン引数を解析する
args = getResolvedOptions(sys.argv, ['DATE'])

# S3にアップロードするための情報
bucket_name = "gs-analysis-platform"

# 銘柄を指定
target = "AAPL"

# yfinanceからデータを取得
data = yf.download(target, period='1d', interval='1d')
#data = yf.download(target, start='2024-12-18', end='2024-12-19')

print(data)

# yfinanceのバグか仕様変更でヘッダーが変わったので、ヘッダーを独自に書き換える
data.index.name = 'date'
#data.columns = ["Adj Close", "Close", "High", "Low", "Open", "Volume"]

expected_cols_5 = ["open", "high", "low", "close", "volume"]
expected_cols_6 = ["open", "high", "low", "close", "adj_close", "volume"]

if data.shape[1] == 5:
    data.columns = expected_cols_5
elif data.shape[1] == 6:
    data.columns = expected_cols_6
else:
    print("予期しない列数:", data.shape[1])
    print(data.columns)

print(data)

# 引数から日付を取得 (例: '20241005')
date_str = args['DATE']

# S3パスを作成
s3_file_name = f"csv/stock_price/{date_str}/stock_price.csv"

# CSVファイルとして保存するための一時ファイルパス
csv_file_path = "/tmp/stock_price.csv"

# CSVファイルに書き込む
#with open(csv_file_path, mode='w', newline='') as file:
#    writer = csv.writer(file)
#    # ヘッダーを作成
#    writer.writerow(["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
#    # データを1行ずつ書き込み
#    for index, row in data.iterrows():
#        writer.writerow([index, row['Open'], row['High'], row['Low'], row['Close'], row['Adj Close'], row['Volume']])
with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)

    if 'adj_close' in data.columns:
        # 6列ある (Open, High, Low, Close, Adj Close, Volume)
        writer.writerow(["date", "open", "high", "low", "close", "adj_close", "volume"])
        for index, row in data.iterrows():
            writer.writerow([
                index,
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['adj_close'],
                row['volume']
            ])
    else:
        # 'Adj Close' 列がない ⇒ 5列だけ
        writer.writerow(["date", "open", "high", "low", "close", "volume"])
        for index, row in data.iterrows():
            writer.writerow([
                index,
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume']
            ])

# S3にファイルをアップロード
s3 = boto3.client('s3')

try:
    # ファイルをS3にアップロード
    s3.upload_file(csv_file_path, bucket_name, s3_file_name)
    print(f"ファイルがS3バケット '{bucket_name}' のパス '{s3_file_name}' に正常にアップロードされました。")
except Exception as e:
    print(f"ファイルのアップロード中にエラーが発生しました: {e}")

# 一時ファイルの削除（必要に応じて）
if os.path.exists(csv_file_path):
    os.remove(csv_file_path)

