import os
from dotenv import load_dotenv
from .model import TrendSegmenterTransformer
from agents_basket.common.base_agent import ClassificationBaseAgent

load_dotenv()


class TrendSegmenterAgent(ClassificationBaseAgent):
    model_name_prefix = "trend_segmenter"
    model_class = TrendSegmenterTransformer

    def load_config(self, config_path):
        import yaml

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
        self.d_model = self.config.get("d_model", 64)
        self.num_classes = self.config.get("num_classes", 3)

    def init_model(self):
        self.model = self.model_class(
            input_dim=self.input_dim,
            sequence_length=self.sequence_length,
            d_model=self.d_model,
            num_classes=self.num_classes
        ).to(self.device)

    def parse_features(self, value: dict) -> bool:
        x = value.get("input")
        y = value.get("target")

        if (
            not x or y is None
            or len(x) != self.sequence_length
            or not all(isinstance(row, list) and len(row) == self.input_dim for row in x)
        ):
            return False

        self.batch_x.append(x)
        self.batch_y.append(y)
        return len(self.batch_x) >= self.batch_size
