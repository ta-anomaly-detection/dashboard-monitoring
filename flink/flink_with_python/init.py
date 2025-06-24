from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import joblib
import pandas as pd

df = pd.read_csv('last_500_processed_data.csv')

scaler = StandardScaler()
df['size'] = scaler.fit_transform(df[['size']])

X = df.drop('detected', axis=1)
y = df['detected']

pca = PCA(n_components=3)
X_pca = pca.fit_transform(X)

joblib.dump(scaler, 'scaler.pkl')
joblib.dump(pca, 'pca_model.pkl')

print("Scaler and PCA model saved successfully to Google Drive.")
