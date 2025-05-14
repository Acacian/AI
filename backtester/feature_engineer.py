import polars as pl

def add_features(df):
    df = df.with_columns([
        pl.col("input").list.eval(pl.element().alias("x"))
            .list.slice(-10)  # 마지막 10개 기준
            .list.mean().alias("ma10"),
        pl.col("input").list.eval(pl.element())
            .list.slice(-10)
            .list.std().alias("std10"),
    ])
    return df