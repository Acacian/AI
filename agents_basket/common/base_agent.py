from abc import ABC, abstractmethod
import os

class BaseAgent(ABC):
    def __init__(self, config_path: str):
        self.load_config(config_path)
        self.init_model()
        self.init_optimizer()

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

    def should_pretrain(self):
        return not os.path.exists(self.model_path)

    def run(self):
        if self.should_pretrain():
            self.run_offline()
        self.run_online()
