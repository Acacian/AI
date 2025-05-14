import polars as pl

def add_labels(df):
    import numpy as np
    # 예시: ma10이 상승하면 1, 하락하면 0 (단순 기준)
    ma = df["ma10"].to_numpy()
    label = (np.roll(ma, -1) > ma).astype(int)
    label[-1] = 0  # 마지막은 미래를 모르니까 0
    df = df.with_columns(pl.Series(name="label", values=label))
    return df