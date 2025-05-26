from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

class NoiseFilterAgent(BaseAgent):
    model_name_prefix = "noise_filter"
    model_class = TransformerAE