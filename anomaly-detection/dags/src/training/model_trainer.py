import io
import joblib
import pandas as pd
import numpy as np

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

import mlflow
import mlflow.pyfunc
from mlflow.models import infer_signature

from src.model.iforestasd.model import IForestASDPyFuncModel
from src.preprocessing.base_preprocessing import LogPreprocessor

def train_initial_model(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='load_data_from_s3', key='s3_data')
    df = pd.read_csv(io.StringIO(data))
    
    preprocessor = LogPreprocessor()
    df = preprocessor.preprocess(df)
    df = select_features(df)

    global X_train, X_test, y_train, y_test

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
    
    pca = PCA(n_components=3)
    X_train_pca = pca.fit_transform(X_train_scaled)
    X_test_pca = pca.transform(X_test_scaled)

    X_train_pca_normal = X_train_pca[y_train == 0]
    X_train_subset = X_train_pca_normal[:1000]
    X_test_subset = X_test_pca[:1000]
    y_test_subset = y_test[:1000]

    mlflow.set_tracking_uri("http://mlflow-server:5000")

    with mlflow.start_run():
        mlflow.set_tag("stage", "initial_training")
        mlflow.set_tag("model_version", "v1")
        mlflow.log_param("model_type", "IForestASD_PyFunc")
        mlflow.log_params({
            "pca_n_components": 3,
            "scaling_columns": cols_to_scale,
        })

        joblib.dump(scaler, "/tmp/scaler.joblib")
        joblib.dump(pca, "/tmp/pca.joblib")
        mlflow.log_artifact("/tmp/scaler.joblib", artifact_path="preprocessing")
        mlflow.log_artifact("/tmp/pca.joblib", artifact_path="preprocessing")

        signature = infer_signature(X_train_subset, y_test_subset)
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=IForestASDPyFuncModel(initial_data=X_train_subset),
            code_path=["/opt/airflow/dags/src"],
            signature=signature
        )

        mlflow.register_model(
            model_uri=f"runs:/{mlflow.active_run().info.run_id}/model",
            name="IForestASD_PyFuncModel"
        )

    best_auc = 0.0

    kwargs['ti'].xcom_push(key='initial_auc', value=best_auc)

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