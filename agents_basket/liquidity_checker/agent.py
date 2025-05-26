from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

class LiquidityCheckerAgent(BaseAgent):
    model_name_prefix = "liquidity_checker"
    model_class = TransformerAE