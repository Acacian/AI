from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

class PatternAEAgent(BaseAgent):
    model_name_prefix = "pattern_ae"
    model_class = TransformerAE