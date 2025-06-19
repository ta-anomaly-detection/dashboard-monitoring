
import pandas as pd
import io
import torch
import numpy as np
from torch.utils.data import DataLoader
from sklearn import metrics
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from torch.autograd import Variable
import math
import mlflow
from mlflow.tracking import MlflowClient

from hyperopt import fmin, tpe, hp, STATUS_OK, Trials
from sklearn.metrics import roc_auc_score

from src.preprocessing.base_preprocessing import LogPreprocessor
from src.model.memstream.model import MemStream
from src.model.memstream.memory_io import save_memory, load_memory

def train_initial_model(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='load_data_from_s3', key='s3_data')
    df = pd.read_csv(io.StringIO(data))
    
    preprocessor = LogPreprocessor()
    df = preprocessor.preprocess(df)
    df = select_features(df)

    global X_train_tensor, X_test_tensor, y_train, y_test

    X = df.drop(columns=['detected'])
    y = df['detected'].replace({'AMAN': 0, 'DICURIGAI': 1, 'BAHAYA': 1})
    labels = (np.array(y)).astype(int)

    total_len = len(X)
    split_idx = int(0.8 * total_len)

    X_train = X[:split_idx]
    X_test = X[split_idx:]
    y_train = labels[:split_idx]
    y_test = labels[split_idx:]

    cols_to_scale = ['size', 'ip']
    cols_to_scale_indices = [X_train.columns.get_loc(col) for col in cols_to_scale if col in X_train.columns]

    scaler = ColumnTransformer(
        transformers=[
            ('size_ip_scaler', StandardScaler(), cols_to_scale_indices)
        ],
        remainder='passthrough'
    )

    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    X_train_tensor = torch.tensor(np.array(X_train_scaled, dtype=np.float32))
    X_test_tensor = torch.tensor(np.array(X_test_scaled, dtype=np.float32))

    best_model, best_auc, best_params = run_hyperopt_tuning()

    save_memory(best_model)

    kwargs['ti'].xcom_push(key='initial_auc', value=best_auc)

def run_hyperopt_tuning():
    space = {
        'beta': hp.uniform('beta', 0.01, 1),
        'memory_len': hp.quniform('memory_len', 512, 4096, 1),
        'lr': hp.loguniform('lr', -5, -1),
        'epochs': hp.quniform('epochs', 1000, 10000, 100),
    }

    trials = Trials()
    mlflow.set_tracking_uri("http://mlflow-server:5000")

    with mlflow.start_run(run_name="hyperopt_memstream_tuning") as parent_run:
        best = fmin(
            fn=objective,
            space=space,
            algo=tpe.suggest,
            max_evals=50,
            trials=trials
        )

        best_params = {
            'beta': best['beta'],
            'memory_len': int(best['memory_len']),
            'lr': best['lr'],
            'epochs': int(best['epochs']),
            'batch_size': 1
        }

        mlflow.log_params({f"best_{k}": v for k, v in best_params.items()})

        best_model, best_auc = train_and_log_final_model(best_params)

        return best_model, best_auc, best_params
    
def objective(params):
    with mlflow.start_run(nested=True):
        N = int(params['memory_len'])
        params_model = {
            'beta': params['beta'],
            'memory_len': N,
            'batch_size': 1,
            'lr': params['lr']
        }

        mlflow.log_params({
            "beta": params['beta'],
            "memory_len": N,
            "lr": params['lr'],
            "epochs": int(params['epochs'])
        })

        model = MemStream(X_train_tensor[0].shape[0], params_model)
        batch_size = params_model['batch_size']

        init_data = X_train_tensor[y_train == 0][:N]
        model.mem_data = init_data

        torch.set_grad_enabled(True)
        model.train_autoencoder(Variable(init_data), epochs=int(params['epochs']))
        torch.set_grad_enabled(False)

        model.initialize_memory(Variable(init_data[:N]))

        err_test = []
        for data in DataLoader(X_test_tensor, batch_size=batch_size):
            output = model(data)
            err_test.append(output)

        scores_test = np.array([
            i.cpu().item() if not math.isnan(i.cpu().item()) and math.isfinite(i.cpu().item()) else 1e10
            for i in err_test
        ])

        auc = roc_auc_score(y_test, scores_test)
        mlflow.log_metric("roc_auc", auc)

        print(f"[Hyperopt] AUC: {auc}, Params: {params}")
        return {'loss': -auc, 'status': STATUS_OK}

def train_and_log_final_model(params):
    mlflow.set_tracking_uri("http://mlflow-server:5000")

    with mlflow.start_run(run_name="best_model_retrain", nested=True):
        mlflow.log_params(params)

        model = MemStream(X_train_tensor[0].shape[0], params)
        N = int(params['memory_len'])

        init_data = X_train_tensor[y_train == 0][:N]
        model.mem_data = init_data

        torch.set_grad_enabled(True)
        model.train_autoencoder(Variable(init_data), epochs=int(params['epochs']))
        torch.set_grad_enabled(False)

        model.initialize_memory(Variable(init_data[:N]))

        scores = []
        for batch in DataLoader(X_test_tensor, batch_size=params['batch_size']):
            output = model(batch)
            score = output.cpu().item() if math.isfinite(output.cpu().item()) else 1e10
            scores.append(score)

        auc = roc_auc_score(y_test, scores)
        mlflow.log_metric("final_roc_auc", auc)

        mlflow.pytorch.log_model(
            model,
            artifact_path="memstream_model",
            code_paths=["/opt/airflow/dags/src"]
        )

        result = mlflow.register_model(
            model_uri=f"runs:/{mlflow.active_run().info.run_id}/memstream_model",
            name="MemStreamModel"
        )

        MlflowClient().transition_model_version_stage(
            name="MemStreamModel",
            version=result.version,
            stage="Production",
            archive_existing_versions=True
        )

        print(f"Registered best model: Version {result.version}, ROC-AUC: {auc:.4f}")
        return model, auc
    
def select_features(df: pd.DataFrame) -> pd.DataFrame:
    method_cols = [
        'method_GET', 'method_HEAD', 'method_POST', 'method_OPTIONS',
        'method_CONNECT', 'method_PROPFIND', 'method_CONECT', 'method_TRACE'
    ]

    status_cols = ['status_2', 'status_3', 'status_4', 'status_5']

    device_cols = ['device_Desktop', 'device_Mobile', 'device_Unknown']

    selected_cols = ['ip', 'size', 'country', 'detected'] + status_cols + method_cols + device_cols

    df_selected = df[selected_cols].copy()

    return df_selected