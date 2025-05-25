from abc import ABC, abstractmethod
import os
import logging

logger = logging.getLogger("BaseAgent")

class BaseAgent(ABC):
    def __init__(self, config_path: str):
        self.load_config(config_path)
        self.init_model()
        self.init_optimizer()

    # 🔹 공통 유틸
    def log(self, msg: str):
        logger.info(f"[{self.__class__.__name__}] {msg}")

    def ensure_dir(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def should_pretrain(self) -> bool:
        return not os.path.exists(self.model_path)

    def export_onnx_if_defined(self):
        if hasattr(self, "export_onnx"):
            try:
                self.export_onnx()
            except Exception as e:
                self.log(f"ONNX export 실패: {e}")

    # 🔹 필수 구현
    @abstractmethod
    def load_config(self, config_path): pass

    @abstractmethod
    def init_model(self): pass

    @abstractmethod
    def init_optimizer(self): pass

    @abstractmethod
    def run_offline(self): pass

    @abstractmethod
    def run_online(self): pass

    @abstractmethod
    def save_model(self): pass

    @abstractmethod
    def load_model(self): pass

    @property
    @abstractmethod
    def model_path(self) -> str: pass

    # 🔹 실행 흐름
    def run(self):
        if self.should_pretrain():
            self.log("🧠 모델 없음 → 오프라인 학습 시작")
            self.run_offline()
            self.save_model()
        else:
            self.log("📦 모델 로딩 시도")
            self.load_model()
        self.run_online()
