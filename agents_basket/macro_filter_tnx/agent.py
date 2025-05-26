from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

class MacroFilterTNXAgent(BaseAgent):
    model_name_prefix = "macro_filter_tnx"
    model_class = TransformerAE