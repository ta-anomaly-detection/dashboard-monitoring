import io
import mlflow
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

from src.model.iforestasd.model import IForestASDPyFuncModel
from src.preprocessing.base_preprocessing import LogPreprocessor

from sklearn.metrics import roc_auc_score


def evaluate_model(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='load_data_from_s3', key='s3_data')
    df = pd.read_csv(io.StringIO(data))

    df = LogPreprocessor().preprocess(df)
    df = select_features(df)

    X = df.drop(columns=['detected'])
    y = df['detected'].replace({'AMAN': 0, 'DICURIGAI': 1, 'BAHAYA': 1}).astype(int)

    cols_to_scale = ['size', 'ip']
    scale_indices = [X.columns.get_loc(c) for c in cols_to_scale if c in X.columns]
    scaler = ColumnTransformer([
        ("scale", StandardScaler(), scale_indices)
    ], remainder="passthrough")

    X_scaled = scaler.fit_transform(X)
    pca = PCA(n_components=3)
    X_pca = pca.fit_transform(X_scaled)

    mlflow.set_tracking_uri("http://mlflow-server:5000")
    model_uri = "models:/IForestASD_PyFuncModel/latest"

    model = mlflow.pyfunc.load_model(model_uri)


    preds = model.predict(X_pca)

    auc = roc_auc_score(y, preds)
    print(f"Evaluated model ROC-AUC on new S3 data: {auc}")

    with mlflow.start_run(run_name="IForest Evaluation on S3 Data"):
        mlflow.log_param("data_source", "S3")
        mlflow.log_metric("eval_auc", auc)

    print(f"Model evaluation completed with AUC: {auc}")
    if auc > 0.8:
        return "skip_retraining"
    return "retrain_model"

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