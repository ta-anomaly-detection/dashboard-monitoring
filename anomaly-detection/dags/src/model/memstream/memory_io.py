import torch
import os

CHECKPOINT_PATH = "/opt/airflow/dags/src/model/memstream/data/memory_checkpoint.pth"

def save_memory(model):
    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)
    torch.save({
        'memory': model.memory,
        'mem_data': model.mem_data,
        'count': model.count,
        'mean': model.mean,
        'std': model.std,
    }, CHECKPOINT_PATH)

def load_memory(model):
    if os.path.exists(CHECKPOINT_PATH):
        checkpoint = torch.load(CHECKPOINT_PATH)
        model.memory = checkpoint['memory']
        model.mem_data = checkpoint['mem_data']
        model.count = checkpoint['count']
        model.mean = checkpoint['mean']
        model.std = checkpoint['std']