FROM python:3.9-slim

WORKDIR /app

# 必要なライブラリをインストール
RUN pip install mysql-connector-python

# スクリプトをコピー
COPY data_generator.py .

# 実行コマンド
CMD ["python", "data_generator.py"]
