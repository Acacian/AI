import os, sys, yaml, logging
from dotenv import load_dotenv
from .model import RiskScorerTransformer
from agents_basket.common.base_agent import ClassificationBaseAgent

load_dotenv()


class RiskScorerAgent(ClassificationBaseAgent):
    model_name_prefix = "risk_scorer"
    model_class = RiskScorerTransformer

    def load_config(self, config_path):
        with open(config_path) as f:
            full_config = yaml.safe_load(f)

        if self.model_name_prefix not in full_config:
            raise ValueError(f"❌ 설정 파일에 '{self.model_name_prefix}' 키가 없습니다")

        self.config = full_config[self.model_name_prefix]

        self.topic = self.config["topic"]
        self.model_base_path = self.config["model_path"]
        self.batch_size = 1 if os.getenv("MODE", "prod").lower() == "test" else self.config.get("batch_size", 32)
        self.learning_rate = self.config.get("learning_rate", 1e-3)
        self.sequence_length = self.config.get("sequence_length", 100)
        self.input_dim = self.config.get("input_dim", 5)
        self.d_model = self.config.get("hidden_size", 64)
        self.num_classes = self.config.get("num_classes", 2)
