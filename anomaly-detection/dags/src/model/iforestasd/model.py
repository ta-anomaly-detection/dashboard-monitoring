import mlflow.pyfunc
import numpy as np
import pandas as pd
from pysad.models import IForestASD

class IForestASDPyFuncModel(mlflow.pyfunc.PythonModel):
    def __init__(self, initial_data=None, window_size=2048):
        self.initial_data = initial_data
        self.window_size = window_size

    def load_context(self, context):
        if self.initial_data is None:
            initial_window = np.random.rand(5, 3)
        else:
            if isinstance(self.initial_data, pd.DataFrame):
                initial_window = self.initial_data.to_numpy()
            else:
                initial_window = np.array(self.initial_data)

        self.model = IForestASD(initial_window_X=initial_window, window_size=self.window_size)

    def predict(self, context, model_input):
        if isinstance(model_input, np.ndarray):
            model_input = pd.DataFrame(model_input)

        scores = []
        for _, row in model_input.iterrows():
            X = np.array(row)
            score = self.model.score_partial(X)
            self.model.fit_partial(X)
            scores.append(score)
        return np.array(scores)