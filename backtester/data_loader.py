import polars as pl

def load_data() -> pl.DataFrame:
    df = pl.read_parquet("data/train_data.parquet")
    return df