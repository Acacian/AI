from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

class OverheatDetectorAgent(BaseAgent):
    model_name_prefix = "overheat_detector"
    model_class = TransformerAE