import torch
import torch.nn as nn

class AEModel(nn.Module):
    def __init__(self, input_size=5, seq_len=100, hidden=64):
        super().__init__()
        self.input_size = input_size
        self.seq_len = seq_len

        self.encoder = nn.Sequential(
            nn.Flatten(),
            nn.Linear(input_size * seq_len, hidden),
            nn.ReLU(),
            nn.Linear(hidden, hidden // 2),
            nn.ReLU()
        )

        self.decoder = nn.Sequential(
            nn.Linear(hidden // 2, hidden),
            nn.ReLU(),
            nn.Linear(hidden, input_size * seq_len),
            nn.Unflatten(1, (seq_len, input_size))
        )

    def forward(self, x):
        z = self.encoder(x)
        out = self.decoder(z)
        return out
