from datetime import datetime
import logging

def extract_values(record):
    if record is None:
        return []

    return [
        record.get('ts', ''),
        record.get('remote_ip', ''),
        record.get('latency_us', 0.0),
        record.get('host', ''),
        record.get('http_method', ''),
        record.get('encoded_http_method', 0),
        record.get('request_uri', ''),
        record.get('url_path', ''),
        record.get('url_query', ''),
        record.get('http_version', ''),
        record.get('encoded_http_version', 0),
        record.get('response_status', 0),
        record.get('encoded_status', 0),
        record.get('response_size', 0),
        record.get('user_agent', ''),
        record.get('device_family', ''),
        record.get('encoded_device', 0),
        record.get('country', ''),
        record.get('encoded_country', 0),
        record.get('referrer', ''),
        record.get('request_id', ''),
        record.get('msg', ''),
        record.get('level', '')
    ]


def clean_ts(ts_str):
    try:
        dt = datetime.fromisoformat(ts_str.replace('Z', ''))  # strip Z
        return dt.strftime('%Y-%m-%d %H:%M:%S')  # ClickHouse suka format ini
    except Exception as e:
        logging.error(f"Failed to clean ts: {ts_str} | Error: {e}")
        return None
