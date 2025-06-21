import logging
import re
from typing import List, Tuple

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import pandas as pd
import re
from typing import List


class LogPreprocessor:
    def __init__(self):
        self.method_labels = {
            'GET': 1, 'HEAD': 2, 'POST': 3, 'OPTIONS': 4,
            'CONNECT': 5, 'PROPFIND': 6, 'CONECT': 7, 'TRACE': 8,
        }
        self.protocol_labels = {
            'HTTP/1.0': 1,
            'HTTP/1.1': 2,
            'hink': 3,
        }
        self.status_labels = {'2': 1, '3': 2, '4': 3, '5': 4}
        self.device_mapping = {
            'Windows': 1, 'Linux': 1, 'Macintosh': 1,
            'Android': 2, 'iPad': 2, 'iPod': 2, 'iPhone': 2,
            'Unknown': 3,
        }

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = self._filter_invalid_and_missing_data(df)
            df = self._split_request_column(df)
            df = self._one_hot_encode_method(df)
            df = self._encode_protocol(df)
            df = self._split_url_column(df)
            df = self._extract_query_array(df)
            df = self._extract_url_tokens(df)
            df = self._one_hot_encode_device(df)
            df = self._encode_country(df)
            df = self._one_hot_encode_status(df)
            df = self._extract_datetime_features(df)
            df = self._convert_ip_to_int(df)
            return df
        except Exception as e:
            logger.exception("Preprocessing failed")
            raise

    def _convert_ip_to_int(self, df: pd.DataFrame) -> pd.DataFrame:
        def ip_to_int(ip: str):
            try:
                octets = ip.split('.')
                if len(octets) != 4:
                    return None
                return sum(int(octet) << (8 * (3 - i)) for i, octet in enumerate(octets))
            except:
                return None

        df['ip'] = df['ip'].astype(str).apply(ip_to_int)
        df = df.dropna(subset=['ip'])
        return df

    def _filter_invalid_and_missing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df['size'] = pd.to_numeric(df.get('size'), errors='coerce')
        df = df.dropna(subset=['size', 'referer', 'browser', 'status', 'request'])
        df['size'] = df['size'].fillna(0)
        return df

    def _split_request_column(self, df: pd.DataFrame) -> pd.DataFrame:
        splits = df['request'].str.split(' ', n=2, expand=True)
        df = df.drop(columns='request')
        df['method'], df['url'], df['protocol'] = splits[0], splits[1], splits[2]
        return df

    def _split_url_column(self, df: pd.DataFrame) -> pd.DataFrame:
        def split_url(url: str):
            if not isinstance(url, str): return '-', '-'
            parts = url.split('?', 1)
            return parts[0], parts[1] if len(parts) > 1 else '-'
        df[['endpoint', 'query']] = df['url'].apply(lambda x: pd.Series(split_url(x)))
        return df

    def _one_hot_encode_method(self, df: pd.DataFrame) -> pd.DataFrame:
        all_methods = ['GET', 'HEAD', 'POST', 'OPTIONS', 'CONNECT', 'PROPFIND', 'CONECT', 'TRACE']

        df['method'] = df['method'].replace('OPTION', 'OPTIONS')

        df_one_hot = pd.get_dummies(df['method'], prefix='method')

        for method in all_methods:
            col_name = f'method_{method}'
            if col_name not in df_one_hot.columns:
                df_one_hot[col_name] = 0

        df_one_hot = df_one_hot[[f'method_{m}' for m in all_methods]]

        df = df.drop(columns=['method']).reset_index(drop=True)
        df_one_hot = df_one_hot.reset_index(drop=True)
        df = pd.concat([df, df_one_hot], axis=1)

        return df

    def _encode_protocol(self, df: pd.DataFrame) -> pd.DataFrame:
        df['protocol'] = df['protocol'].map(self.protocol_labels)
        return df.dropna(subset=['protocol'])

    def _one_hot_encode_status(self, df: pd.DataFrame) -> pd.DataFrame:
        df['status'] = df['status'].astype(str).str[0]

        df_status = pd.get_dummies(df['status'], prefix='status')

        all_status_prefixes = ['status_2', 'status_3', 'status_4', 'status_5']
        for col in all_status_prefixes:
            if col not in df_status.columns:
                df_status[col] = 0

        df_status = df_status[all_status_prefixes]

        df = df.drop(columns=['status']).reset_index(drop=True)
        df_status = df_status.reset_index(drop=True)
        df = pd.concat([df, df_status], axis=1)

        return df

    def _encode_country(self, df: pd.DataFrame) -> pd.DataFrame:
        df['country'] = df['country'].apply(lambda x: 1 if x == 'Indonesia' else 0)
        return df

    def _extract_query_array(self, df: pd.DataFrame) -> pd.DataFrame:
        def process_query(query: str) -> List[str]:
            if not isinstance(query, str) or query == '-':
                return []
            return [pair.split('=')[0] for pair in query.split('&') if '=' in pair]
        df['query_array'] = df['query'].apply(process_query)
        return df

    def _extract_url_tokens(self, df: pd.DataFrame) -> pd.DataFrame:
        df['url_tokens'] = df['endpoint'].apply(lambda x: x.split('/')[1:] if isinstance(x, str) else [])
        return df

    def _one_hot_encode_device(self, df: pd.DataFrame) -> pd.DataFrame:
        device_patterns = ['Windows', 'Linux', 'Macintosh', 'Android', 'iPad', 'iPod', 'iPhone']

        def extract_device_category(user_agent: str) -> str:
            if not isinstance(user_agent, str):
                return 'Unknown'
            for pattern in device_patterns:
                if re.search(pattern, user_agent, re.I):
                    if pattern in ['Windows', 'Linux', 'Macintosh']:
                        return 'Desktop'
                    elif pattern in ['Android', 'iPad', 'iPod', 'iPhone']:
                        return 'Mobile'
            return 'Unknown'

        df['browser'] = df['browser'].fillna('-')

        df['device_category'] = df['browser'].apply(extract_device_category)

        df['browser'] = df['browser'].str.split('/').str[0]

        df_device = pd.get_dummies(df['device_category'], prefix='device')

        all_devices = ['device_Desktop', 'device_Mobile', 'device_Unknown']
        for col in all_devices:
            if col not in df_device.columns:
                df_device[col] = 0

        df_device = df_device[all_devices]

        df = df.drop(columns=['device_category']).reset_index(drop=True)
        df_device = df_device.reset_index(drop=True)

        df = pd.concat([df, df_device], axis=1)
        return df


    def _extract_datetime_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df = df.dropna(subset=['datetime'])
        df = df.drop(columns=['gmt'], errors='ignore')
        df['year'] = df['datetime'].dt.year
        df['month'] = df['datetime'].dt.month
        df['day'] = df['datetime'].dt.day
        df['hour'] = df['datetime'].dt.hour
        df['minute'] = df['datetime'].dt.minute
        df['second'] = df['datetime'].dt.second
        return df.drop(columns=['datetime'])