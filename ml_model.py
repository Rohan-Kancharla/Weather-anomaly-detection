import numpy as np
from sklearn.ensemble import IsolationForest

"""
Isolation Forest Anomaly Detector

- Trains on simulated "normal" weather data at initialization.
- Predicts whether incoming (temp, humidity, wind) vector is Normal/Anomaly.
"""


class AnomalyDetector:
    def __init__(self, random_state: int = 42):
        self.model = IsolationForest(
            n_estimators=150,
            contamination=0.05,  # expected anomaly proportion
            random_state=random_state,
        )
        self._train_on_simulated_normal()

    def _train_on_simulated_normal(self, n_samples: int = 3000):
        # Simulate normal distributions for weather features
        temp = np.random.normal(loc=30.0, scale=2.0, size=n_samples)
        humidity = np.random.normal(loc=55.0, scale=5.0, size=n_samples)
        wind = np.random.normal(loc=3.0, scale=1.0, size=n_samples)

        X = np.column_stack([temp, humidity, wind])
        self.model.fit(X)

    def predict(self, features):
        """
        Predict anomaly status.
        Args:
            features: iterable or array-like [temp, humidity, wind]
        Returns:
            status: 'Normal' or 'Anomaly'
            score: anomaly score (lower = more anomalous)
        """
        x = np.array(features, dtype=float).reshape(1, -1)
        pred = self.model.predict(x)[0]  # 1 normal, -1 anomaly
        score = self.model.decision_function(x)[0]
        status = "Anomaly" if pred == -1 else "Normal"
        return status, float(score)