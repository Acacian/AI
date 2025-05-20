import os
import duckdb
import polars as pl
from pathlib import Path

DATA_ROOT = "data"
OUTPUT_ROOT = "duckdb"
os.makedirs(OUTPUT_ROOT, exist_ok=True)

def merge_parquet_dir(interval: str):
    data_path = Path(DATA_ROOT) / interval
    db_path = Path(OUTPUT_ROOT) / f"merged_{interval}.db"
    con = duckdb.connect(str(db_path))

    files_by_symbol = {}

    # 모든 parquet 파일을 symbol별로 그룹화
    for file in data_path.glob("*.parquet"):
        symbol = file.stem.split("_")[0]
        files_by_symbol.setdefault(symbol, []).append(file)

    for symbol, files in files_by_symbol.items():
        files_sorted = sorted(files)  # 날짜순 정렬
        file_list = ", ".join(f"'{str(f)}'" for f in files_sorted)

        table_name = symbol.lower() + f"_{interval}"
        print(f"📥 병합 중: {symbol} ({len(files)}개 파일) → {table_name}")

        query = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_parquet([{file_list}]);
        """
        con.execute(query)

    con.close()
    print(f"✅ DuckDB 저장 완료: {db_path}")

if __name__ == "__main__":
    merge_parquet_dir("1m")   # 필요 시 반복 실행: "1d", "5m", 등
