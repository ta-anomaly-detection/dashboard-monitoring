import logging
import re
import requests
import joblib

from pyflink.datastream.functions import MapFunction
from pyflink.common import Row


class LogPreprocessor(MapFunction):
    def __init__(self):
        self.scaler = joblib.load('scaler.pkl')
        self.pca = joblib.load('pca_model.pkl')
        self.method_labels = ['GET', 'HEAD', 'POST', 'OPTIONS', 'CONNECT', 'PROPFIND', 'CONECT', 'TRACE']
        self.protocol_labels = {'HTTP/1.0': 1, 'HTTP/1.1': 2, 'hink': 3}
        self.status_prefixes = ['2', '3', '4', '5']
        self.device_patterns = ['Windows', 'Linux', 'Macintosh', 'Android', 'iPad', 'iPod', 'iPhone']

    def _ip_to_int(self, ip):
        try:
            octets = ip.split('.')
            if len(octets) != 4:
                return 0
            return sum(int(octet) << (8 * (3 - i)) for i, octet in enumerate(octets))
        except:
            return 0

    def _device_category(self, user_agent):
        if not isinstance(user_agent, str):
            return "Unknown"
        for pattern in self.device_patterns:
            if re.search(pattern, user_agent, re.I):
                if pattern in ['Windows', 'Linux', 'Macintosh']:
                    return "Desktop"
                return "Mobile"
        return "Unknown"

    def _encode_status(self, status_code):
        status_prefix = str(status_code)[0]
        return [1 if status_prefix == s else 0 for s in self.status_prefixes]

    def _encode_method(self, method):
        return [1 if method == m else 0 for m in self.method_labels]

    def _encode_device(self, category):
        return [
            1 if category == "Desktop" else 0,
            1 if category == "Mobile" else 0,
            1 if category == "Unknown" else 0
        ]
    
    def _send_to_api(self, pca_vec):
        pca_vec = [float(x) for x in pca_vec]

        payload = {
            "dataframe_split": {
                "columns": ["pca_1", "pca_2", "pca_3"],
                "data": [pca_vec]
            }
        }
        try:
            response = requests.post("http://mlflow-server:1234/invocations", json=payload)
            logging.info(f"API response: {response.status_code}")
        except Exception as e:
            logging.error(f"API call failed: {e}")


    def map(self, row: Row) -> Row:
        try:
            size = row.responseByte
            country = 1 if getattr(row, 'country', '') == "Indonesia" else 0
            method = row.method
            device = self._device_category(row.userAgent)

            status_flags = self._encode_status(row.responseCode)
            method_flags = self._encode_method(method)
            device_flags = self._encode_device(device)

            features = [
                size, country,
                *status_flags, *method_flags, *device_flags
            ]

            size_index = 0
            original_size = features[size_index]

            scaled_size = self.scaler.transform([[original_size]])[0][0]

            features[size_index] = scaled_size

            scaled = features
            vec3 = self.pca.transform([scaled])[0]
            vec3_list = [float(v) for v in vec3]

            self._send_to_api(vec3_list)

            return Row(*vec3)

        except Exception as e:
            import logging
            import traceback
            logging.debug(f"Feature vector: {features}")
            logging.debug(f"Scaled: {scaled}")
            logging.debug(f"PCA: {vec3}")
            logging.error(f"Failed preprocessing: {e}\n{traceback.format_exc()} | Row: {row}")
            return Row(None, None, None)
