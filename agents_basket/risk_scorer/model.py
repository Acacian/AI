import torch
import torch.nn as nn

class RiskScorerTransformer(nn.Module):
    def __init__(self, input_dim, sequence_length, hidden_size, num_classes, nhead=4, num_layers=2):
        super().__init__()
        self.input_proj = nn.Linear(input_dim, hidden_size)
        self.pos_embedding = nn.Parameter(torch.randn(1, sequence_length, hidden_size))

        encoder_layer = nn.TransformerEncoderLayer(d_model=hidden_size, nhead=nhead, batch_first=True)
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)

        self.classifier = nn.Linear(hidden_size, num_classes)

    def forward(self, x):
        x_proj = self.input_proj(x) + self.pos_embedding
        encoded = self.encoder(x_proj)
        return self.classifier(encoded[:, -1])
