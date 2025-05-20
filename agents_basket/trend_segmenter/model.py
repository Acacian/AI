import torch
import torch.nn as nn

class TrendSegmenterTransformer(nn.Module):
    def __init__(self, input_dim, sequence_length, d_model, num_classes):
        super().__init__()
        self.input_proj = nn.Linear(input_dim, d_model)
        self.pos_embedding = nn.Parameter(torch.randn(1, sequence_length, d_model))

        encoder_layer = nn.TransformerEncoderLayer(d_model=d_model, nhead=4, batch_first=True)
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=2)

        self.classifier = nn.Linear(d_model, num_classes)

    def forward(self, x):  # x: [B, T, D]
        x = self.input_proj(x) + self.pos_embedding  # [B, T, d_model]
        encoded = self.encoder(x)  # [B, T, d_model]
        return self.classifier(encoded[:, -1])  # 마지막 시점만 사용 → [B, num_classes]
