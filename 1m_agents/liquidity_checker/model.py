import torch.nn as nn

class AEModel(nn.Module):
    def __init__(self, input_size=5, seq_len=100, hidden_size=64):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Flatten(),
            nn.Linear(input_size * seq_len, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size // 2),
        )
        self.decoder = nn.Sequential(
            nn.Linear(hidden_size // 2, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, input_size * seq_len),
            nn.Unflatten(1, (seq_len, input_size)),
        )

    def forward(self, x):
        z = self.encoder(x)
        return self.decoder(z)
