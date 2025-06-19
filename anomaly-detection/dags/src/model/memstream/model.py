import torch
import torch.nn as nn
import torch.optim as optim
from torch.autograd import Variable

class MemStream(nn.Module):
    def __init__(self, in_dim, params):
        super(MemStream, self).__init__()
        self.params = params
        self.in_dim = in_dim
        self.out_dim = in_dim*2
        self.memory_len = params['memory_len']
        self.max_thres = torch.tensor(params['beta'])
        self.memory = torch.randn(self.memory_len, self.out_dim)
        self.mem_data = torch.randn(self.memory_len, self.in_dim)
        self.memory.requires_grad = False
        self.mem_data.requires_grad = False
        self.batch_size = params['batch_size']
        self.num_mem_update = 0
        self.encoder = nn.Sequential(
            nn.Linear(self.in_dim, self.out_dim),
            nn.Tanh(),
        )
        self.decoder = nn.Sequential(
            nn.Linear(self.out_dim, self.in_dim)
        )
        self.clock = 0
        self.last_update = -1
        self.optimizer = torch.optim.Adam(self.parameters(), lr=params['lr'])
        self.loss_fn = nn.MSELoss()
        self.count = 0
        self.mean = torch.zeros(self.in_dim)
        self.std = torch.ones(self.in_dim)

    
    def train_autoencoder(self, data, epochs):
        self.mean, self.std = data.mean(0), data.std(0)
        new = (data - self.mean) / self.std
        new[:, self.std == 0] = 0
        new = Variable(new)
        for epoch in range(epochs):
            self.optimizer.zero_grad()
            output = self.decoder(self.encoder(new + 0.001*torch.randn_like(new)))
            loss = self.loss_fn(output, new)
            loss.backward()
            self.optimizer.step()


    def update_memory(self, output_loss, encoder_output, data):
        if encoder_output.ndim == 1:
            encoder_output = encoder_output.unsqueeze(0)
        if data.ndim == 1:
            data = data.unsqueeze(0)

        if output_loss <= self.max_thres:
            start_pos = self.count % self.memory_len
            end_pos = start_pos + encoder_output.size(0)

            if end_pos > self.memory_len:
                wrap_around_len = end_pos - self.memory_len
                self.memory[start_pos:self.memory_len] = encoder_output[:self.memory_len - start_pos]
                self.memory[0:wrap_around_len] = encoder_output[self.memory_len - start_pos:]
                self.mem_data[start_pos:self.memory_len] = data[:self.memory_len - start_pos]
                self.mem_data[0:wrap_around_len] = data[self.memory_len - start_pos:]
            else:
                self.memory[start_pos:end_pos] = encoder_output
                self.mem_data[start_pos:end_pos] = data

            self.mean, self.std = self.mem_data.mean(0), self.mem_data.std(0)
            self.count += encoder_output.size(0)
            return 1
        return 0

    def initialize_memory(self, x):
        if x.ndim == 1:
            x = x.unsqueeze(0)

        new = (x - self.mean) / self.std
        new[:, self.std == 0] = 0

        encoded_data = self.encoder(new)

        data_len = min(x.size(0), self.memory_len)
        self.memory[:data_len] = encoded_data[:data_len]
        self.mem_data[:data_len] = x[:data_len]

        self.memory.requires_grad = False
        self.mem_data.requires_grad = False

        self.count = data_len
        self.mean, self.std = self.mem_data[:data_len].mean(0), self.mem_data[:data_len].std(0)


    def forward(self, x):
        if x.ndim == 1:
            x = x.unsqueeze(0)

        new = (x - self.mean) / self.std
        new[:, self.std == 0] = 0
        encoder_output = self.encoder(new)

        if self.count == 0:
            return torch.tensor(float('inf'))

        distances = torch.cdist(encoder_output, self.memory[:self.count], p=1)
        loss_values = distances.min(dim=1)[0]

        for i in range(x.size(0)):
            if torch.isfinite(loss_values[i]):
                 self.update_memory(loss_values[i], encoder_output[i], x[i])

        return loss_values