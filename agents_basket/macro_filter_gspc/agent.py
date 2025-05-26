from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

class MacroFilterGSPCAgent(BaseAgent):
    model_name_prefix = "macro_filter_gspc"
    model_class = TransformerAE