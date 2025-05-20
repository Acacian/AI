import torch
import torch.nn as nn

class TransformerAE(nn.Module):
    def __init__(self, input_dim=5, sequence_length=100, d_model=64, nhead=4, num_layers=2):
        super().__init__()
        self.input_proj = nn.Linear(input_dim, d_model)
        self.pos_embedding = nn.Parameter(torch.randn(1, sequence_length, d_model))

        encoder_layer = nn.TransformerEncoderLayer(d_model=d_model, nhead=nhead, batch_first=True)
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)

        self.decoder = nn.Sequential(
            nn.Linear(d_model, d_model),
            nn.ReLU(),
            nn.Linear(d_model, input_dim)
        )

    def forward(self, x):  # [B, T, D]
        x_proj = self.input_proj(x) + self.pos_embedding
        encoded = self.encoder(x_proj)
        return self.decoder(encoded)
