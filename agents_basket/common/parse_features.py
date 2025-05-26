from typing import List, Dict
import numpy as np
import pandas as pd

def default_parser(value: Dict) -> List[List[float]]:
    return value.get("input", [])

def noise_filter_parser(value: Dict) -> List[List[float]]:
    data = value.get("input", [])
    if not isinstance(data, list) or not data:
        return []

    arr = np.array(data)
    if arr.ndim != 2 or arr.shape[0] < 30:
        return []

    close = arr[:, -1]
    df = pd.DataFrame({'close': close})
    df['ema_10'] = df['close'].ewm(span=10).mean()
    df['ema_30'] = df['close'].ewm(span=30).mean()
    df['std_10'] = df['close'].rolling(window=10).std()
    df = df.dropna()

    return df[['close', 'ema_10', 'ema_30', 'std_10']].to_numpy().tolist()

def liquidity_parser(value: Dict) -> List[List[float]]:
    data = value.get("input", [])
    if not isinstance(data, list) or not data:
        return []

    result = []
    for row in data:
        if len(row) < 5:
            continue
        open_, high, low, close, volume = row[:5]
        spread = high - low
        result.append([open_, high, low, close, volume, spread])

    return result

def macro_parser(value: Dict) -> List[List[float]]:
    return value.get("input", [])

def risk_scorer_parser(value: Dict) -> List[List[float]]:
    data = value.get("input", [])
    if not isinstance(data, list) or not data:
        return []

    arr = np.array(data)
    if arr.ndim != 2:
        return []

    mean = arr.mean(axis=0)
    std = arr.std(axis=0)
    return ((arr - mean) / (std + 1e-8)).tolist()

def trend_segmenter_parser(value: Dict) -> List[List[float]]:
    data = value.get("input", [])
    if not isinstance(data, list) or len(data) < 2:
        return []

    arr = np.array(data)
    if arr.ndim != 2:
        return []

    close = arr[:, -1]
    slope = np.diff(close, prepend=close[0])
    return [list(row) + [s] for row, s in zip(arr, slope)]

def orderbook_parser(value: Dict) -> List[List[float]]:
    """
    Orderbook AE: 호가 imbalance, spread, depth sum 등 계산
    """
    data = value.get("input", [])
    if not isinstance(data, list) or not data:
        return []

    result = []
    for row in data:
        if len(row) < 20:
            continue  # 예: 10 bid + 10 ask
        bids = row[:10]
        asks = row[10:20]

        bid_sum = sum(bids)
        ask_sum = sum(asks)
        spread = asks[0] - bids[0]  # best ask - best bid
        imbalance = (bid_sum - ask_sum) / (bid_sum + ask_sum + 1e-8)

        # 예: [best_bid, best_ask, spread, imbalance]
        result.append([bids[0], asks[0], spread, imbalance])

    return result

def volume_ae_parser(value: Dict) -> List[List[float]]:
    """
    Volume AE: 거래량 이동평균 대비 편차, 변화율 등 계산
    """
    import pandas as pd
    data = value.get("input", [])
    if not isinstance(data, list) or not data:
        return []

    df = pd.DataFrame(data, columns=["open", "high", "low", "close", "volume"])
    if len(df) < 10:
        return []

    df["vol_ma10"] = df["volume"].rolling(window=10).mean()
    df["vol_std10"] = df["volume"].rolling(window=10).std()
    df["vol_zscore"] = (df["volume"] - df["vol_ma10"]) / (df["vol_std10"] + 1e-8)
    df["vol_pct_change"] = df["volume"].pct_change().fillna(0)

    df = df.dropna()
    return df[["volume", "vol_ma10", "vol_zscore", "vol_pct_change"]].to_numpy().tolist()

# 전략 매핑
FEATURE_PARSERS = {
    "noise_filter": noise_filter_parser,
    "liquidity_checker": liquidity_parser,
    "risk_scorer": risk_scorer_parser,
    "trend_segmenter": trend_segmenter_parser,
    "orderbook_ae": default_parser,
    "overheat_detector": default_parser,
    "pattern_ae": default_parser,
    "volatility_watcher": default_parser,
    "volume_ae": default_parser,
    "orderbook_ae": orderbook_parser,
    "volume_ae": volume_ae_parser,
}

# prefix 기반 매핑 예: macro_filter_gold → macro_parser
def get_parser_by_prefix(prefix: str):
    if prefix.startswith("macro_filter"):
        return macro_parser
    return FEATURE_PARSERS.get(prefix, default_parser)
