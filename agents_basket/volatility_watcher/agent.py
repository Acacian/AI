from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

class VolatilityWatcherAgent(BaseAgent):
    model_name_prefix = "volatility_watcher"
    model_class = TransformerAE