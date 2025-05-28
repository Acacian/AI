import os, sys, yaml, logging
from dotenv import load_dotenv
from .model import RiskScorerTransformer
from agents_basket.common.base_agent import ClassificationBaseAgent

load_dotenv()

class RiskScorerAgent(ClassificationBaseAgent):
    model_name_prefix = "risk_scorer"
    model_class = RiskScorerTransformer

    def load_config(self, config_path):
        super().load_config(config_path) 
        self.num_classes = self.config.get("num_classes", 2) 