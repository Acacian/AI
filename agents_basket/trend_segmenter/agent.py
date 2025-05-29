import os
from dotenv import load_dotenv
from .model import TrendSegmenterTransformer
from agents_basket.common.base_agent import ClassificationBaseAgent

load_dotenv()

class TrendSegmenterAgent(ClassificationBaseAgent):
    model_name_prefix = "trend_segmenter"
    model_class = TrendSegmenterTransformer

    def load_config(self, config_path):
        super().load_config(config_path)
        self.num_classes = self.config.get("num_classes", 3)