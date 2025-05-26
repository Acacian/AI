from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

class MacroFilterGoldAgent(BaseAgent):
    model_name_prefix = "macro_filter_gold"
    model_class = TransformerAE