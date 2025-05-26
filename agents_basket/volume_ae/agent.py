from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent


class VolumeAEAgent(BaseAgent):
    model_name_prefix = "volume_ae"
    model_class = TransformerAE
