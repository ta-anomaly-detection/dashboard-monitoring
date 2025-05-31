import re
from pyflink.table.udf import udf

@udf(result_type='ARRAY<STRING>')
def split_request(request: str):
    if not request:
        return ["", "", ""]
    parts = request.split(" ", 2)
    while len(parts) < 3:
        parts.append("")
    return parts

@udf(result_type='ARRAY<STRING>')
def split_url(url: str):
    if not url:
        return ["", "-"]
    parts = url.split("?", 1)
    return [parts[0], parts[1]] if len(parts) > 1 else [parts[0], "-"]

@udf(result_type='INT')
def encode_method(method: str):
    mapping = {
        'GET': 1, 'HEAD': 2, 'POST': 3, 'OPTIONS': 4,
        'CONNECT': 5, 'PROPFIND': 6, 'CONECT': 7, 'TRACE': 8,
    }
    method = 'OPTIONS' if method == 'OPTION' else method
    return mapping.get(method, None)

@udf(result_type='INT')
def encode_protocol(protocol: str):
    return {'HTTP/1.0': 1, 'HTTP/1.1': 2, 'hink': 3}.get(protocol, None)

@udf(result_type='INT')
def encode_status(status: int):
    if not status:
        return None
    status = str(status)
    if status.startswith('2'):
        return 1
    elif status.startswith('3'):
        return 2
    elif status.startswith('4'):
        return 3
    elif status.startswith('5'):
        return 4
    else:
        return 5
    
@udf(result_type='INT')
def encode_country(country: str):
    return 1 if country == 'Indonesia' else 0

@udf(result_type='STRING')
def extract_device(ua: str):
    if not ua:
        return 'Unknown'
    for pattern in ['Windows', 'Linux', 'Android', 'iPad', 'iPod', 'iPhone', 'Macintosh']:
        if re.search(pattern, ua, re.I):
            return pattern
    return 'Unknown'

@udf(result_type='INT')
def map_device(device: str):
    return {
        'Windows': 1, 'Linux': 1, 'Macintosh': 1,
        'Android': 2, 'iPad': 2, 'iPod': 2, 'iPhone': 2,
        'Unknown': 3
    }.get(device, 3)

@udf(result_type='FLOAT')
def parse_latency(latency_str):
    """Convert latency string to microseconds (as float)."""
    if not latency_str:
        return 0.0
        
    numeric_part = re.match(r'([0-9\.\-]+)', latency_str)
    if not numeric_part:
        return 0.0
        
    value = float(numeric_part.group(1))
    
    if 'µs' in latency_str:
        return value
    elif 'ms' in latency_str:
        return value * 1000
    elif 's' in latency_str:
        return value * 1000000
    else:
        return value

def register_udfs(t_env):
    """Register all UDFs with the given table environment."""
    t_env.create_temporary_system_function("encode_method", encode_method)
    t_env.create_temporary_system_function("encode_protocol", encode_protocol)
    t_env.create_temporary_system_function("encode_status", encode_status)
    t_env.create_temporary_system_function("extract_device", extract_device)
    t_env.create_temporary_system_function("map_device", map_device)
    t_env.create_temporary_system_function("encode_country", encode_country)
    t_env.create_temporary_system_function("parse_latency", parse_latency)
    t_env.create_temporary_system_function("split_request", split_request)
    t_env.create_temporary_system_function("split_url", split_url)
