import os, re
import duckdb
import polars as pl
from pathlib import Path
from collector.database.backfill import kafka_safe_symbol

DATA_ROOT = "data"
OUTPUT_ROOT = "duckdb"
os.makedirs(OUTPUT_ROOT, exist_ok=True)

def extract_symbol_from_filename(filename: str) -> str:
    """
    filename: 'gspc_2025-05-29.parquet' 또는 '^GSPC_2025-03-03.parquet'
    return: 'gspc' 또는 'gspc' (정규화된 심볼명)
    """
    stem = Path(filename).stem
    # 날짜 형식 제거
    if re.match(r".+_\d{4}-\d{2}-\d{2}$", stem):
        symbol_part = stem.rsplit("_", 1)[0]
    else:
        symbol_part = stem
    return kafka_safe_symbol(symbol_part)

def merge_parquet_dir(interval: str):
    data_path = Path(DATA_ROOT) / interval
    db_path = Path(OUTPUT_ROOT) / f"merged_{interval}.db"
    con = duckdb.connect(str(db_path))

    files_by_symbol = {}

    # 모든 parquet 파일을 symbol별로 그룹화
    for file in data_path.glob("*.parquet"):
        symbol = extract_symbol_from_filename(file.name)
        files_by_symbol.setdefault(symbol, []).append(file)

    for symbol, files in files_by_symbol.items():
        files_sorted = sorted(files)  # 날짜순 정렬
        file_list = ", ".join(f"'{str(f)}'" for f in files_sorted)

        table_name = f'"{symbol}_{interval}"'  # 정규화된 이름 사용
        print(f"📥 병합 중: {symbol} ({len(files)}개 파일) → {table_name}")

        query = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_parquet([{file_list}]);
        """
        con.execute(query)

    con.close()
    print(f"✅ DuckDB 저장 완료: {db_path}")

if __name__ == "__main__":
    merge_parquet_dir("1d")  # 필요 시 "1m", "5m", 등 반복 실행