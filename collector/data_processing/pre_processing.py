import numpy as np

def preprocess_ohlcv(features, log_volume=True, normalize=True):
    """
    OHLCV 데이터를 전처리합니다.

    Args:
        features (list[list[float]]): 시계열 OHLCV 데이터, shape = [T, D]
        log_volume (bool): True이면 volume에 log1p 적용
        normalize (bool): True이면 z-score 정규화 적용

    Returns:
        list[list[float]]: 전처리된 시계열 데이터
    """
    x = np.array(features, dtype=np.float32)  # [T, D]

    if log_volume:
        x[:, 4] = np.log1p(x[:, 4])  # volume만 log 처리

    if normalize:
        mean = x.mean(axis=0)
        std = x.std(axis=0) + 1e-6  # 0분산 방지용 epsilon
        x = (x - mean) / std

    return x.tolist()