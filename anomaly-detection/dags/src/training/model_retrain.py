import mlflow
import torch
import pandas as pd
import numpy as np
from sklearn import metrics
from torch.utils.data import DataLoader
from torch.autograd import Variable


def evaluate_model(model, data: torch.FloatTensor, batch_size: int = 1):
    model.eval()
    data_loader = DataLoader(data, batch_size=batch_size)
    scores = []
    for batch in data_loader:
        with torch.no_grad():
            output = model(Variable(batch))
            score = output.cpu().item()
            scores.append(score)
    return np.array(scores)


def check_model_performance():
    # Development puporse only
    columns = ['country', 'size', 'method', 'device', 'status']
    rows = [
        {'country': 1, 'size': 28691, 'method': 1, 'device': 1, 'status': 1},
        {'country': 0, 'size': 5198,  'method': 1, 'device': 1, 'status': 1},
        {'country': 0, 'size': 11364,  'method': 1, 'device': 1, 'status': 1},
        {'country': 1, 'size': 0, 'method': 3, 'device': 1, 'status': 2},
        {'country': 0, 'size': 18802,  'method': 1, 'device': 1, 'status': 1},
        {'country': 0, 'size': 1892,  'method': 1, 'device': 1, 'status': 1},
    ]

    df = pd.DataFrame(rows, columns=columns)

    labels = np.array([0, 1, 1, 0, 1, 1])
    features = torch.FloatTensor(df[['country', 'size', 'method', 'device', 'status']].values)

    # ==========

    model_name = "MemStreamModel"

    mlflow.set_tracking_uri("http://mlflow-server:5000")
    model = mlflow.pytorch.load_model(f"models:/{model_name}/latest")

    print("Model loaded successfully!")

    print("Data loaded successfully!")

    scores = evaluate_model(model, features)

    auc = metrics.roc_auc_score(labels, scores)

    print(f"Model ROC-AUC on evaluation data: {auc:.4f}")
    if auc < 0.85:
        return "retrain_model"
    return "skip_retraining"