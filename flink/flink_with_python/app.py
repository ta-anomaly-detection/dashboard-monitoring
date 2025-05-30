import os
import requests

from pyflink.common import WatermarkStrategy, Encoder, Row, TypeInformation
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, RollingPolicy, OutputFileConfig
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.configuration import Configuration

from pyflink.common.typeinfo import Types, BasicTypeInfo

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udf
import re

import json
import logging
from pyflink.datastream.functions import MapFunction, FlatMapFunction, SinkFunction
from pyflink.common import WatermarkStrategy, Row
import requests
import clickhouse_driver
from clickhouse_driver import Client
from datetime import datetime

def call_api(row):
    try:
        response = requests.post(
            "http://web:8000/api/prediction/predict",  # real URL
            json={
              "size": row[0],
              "country": row[1],
              "method": row[2],
              "device": row[3],
              "status": row[4]
            },
            timeout=5
        )
        return f"✅ {response.status_code}: {response.text}"
    except Exception as e:
        return f"❌ Error: {str(e)}"

class ParseJson(MapFunction):
    def map(self, value: str) -> Row:
        try:
            outer_data = json.loads(value)
            
            if "message" in outer_data:
                data = json.loads(outer_data["message"])
            else:
                data = outer_data
            
            return Row(
                data.get("level"),
                data.get("ts"),
                data.get("caller"),
                data.get("msg"),
                data.get("remote_ip"),
                data.get("latency"),
                data.get("host"),
                data.get("http_method"),
                data.get("request_uri"),
                data.get("http_version"),
                int(data.get("response_status", 0)),
                int(data.get("response_size", 0)),
                data.get("referrer"),
                data.get("request_body"),
                data.get("request_time"),
                data.get("user_agent"),
                data.get("request_id")
            )
        except Exception as e:
            logging.error(f"Failed to parse JSON: {e} | Raw message: {value}")
            logging.error(f"Message preview: {value[:200]}...")
            return Row(*[None] * 16)  

        
row_type = Types.ROW_NAMED(
    ["level", "ts", "caller", "msg", "remote_ip", "latency", "host", "http_method", "request_uri", "http_version",
     "response_status", "response_size", "referrer", "request_body", "request_time", "user_agent", "request_id"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.STRING()]
)

# UDFs
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

@udf(result_type='ARRAY<STRING>')
def extract_query_array(query: str):
    if not query or query == '-':
        return []
    return [pair.split('=')[0] for pair in query.split('&')]

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


class CombineProcessedWithRaw(MapFunction):
    def map(self, row):
        try:
            # Extract processed features
            response_size = row[0]
            encoded_method = row[1]
            encoded_protocol = row[2] 
            encoded_status = row[3]
            encoded_device = row[4]
            encoded_country = row[5]
            parsed_latency = row[6]
            
            # Create a combined record for anomaly detection and storage
            combined_record = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'size': response_size,
                'method': encoded_method,
                'status': encoded_status,
                'device': encoded_device,
                'country': encoded_country,
                'latency': parsed_latency,
                'protocol': encoded_protocol,
                'raw_request_parts': str(row[7]),
                'raw_url_parts': str(row[8]),
                'raw_query_params': str(row[9])
            }
            
            return combined_record
        except Exception as e:
            logging.error(f"Failed to combine data: {e}")
            return None

class ClickHouseSink(SinkFunction):
    def __init__(self, host, port, database, table):
        self.host = host
        self.port = port
        self.database = database
        self.table = table
        self.client = None
        
    def open(self, config):
        try:
            self.client = Client(
                host=self.host,
                port=self.port,
                database=self.database
            )
            logging.info(f"Connected to ClickHouse at {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logging.error(f"ClickHouse connection failed: {str(e)}")
            raise
        
    def invoke(self, value, context):
        try:
            if value is None:
                return
                
            # Send prediction request to the web API
            try:
                response = requests.post(
                    "http://web:8000/api/prediction/predict",
                    json={
                        "size": value['size'],
                        "country": value['country'],
                        "method": value['method'],
                        "device": value['device'],
                        "status": value['status']
                    },
                    timeout=5
                )
                
                # Add prediction result to the record
                if response.status_code == 200:
                    prediction = response.json()
                    value['anomaly_score'] = prediction.get('score', 0)
                    value['is_anomaly'] = prediction.get('is_anomaly', False)
                else:
                    value['anomaly_score'] = 0
                    value['is_anomaly'] = False
                    
            except Exception as e:
                logging.error(f"API call failed: {str(e)}")
                value['anomaly_score'] = 0
                value['is_anomaly'] = False
                
            # Insert data into ClickHouse
            self.client.execute(
                f"INSERT INTO {self.table} VALUES",
                [{
                    'timestamp': value['timestamp'],
                    'size': value['size'],
                    'method': value['method'],
                    'status': value['status'],
                    'device': value['device'],
                    'country': value['country'],
                    'latency': value['latency'],
                    'protocol': value['protocol'],
                    'request_parts': value['raw_request_parts'],
                    'url_parts': value['raw_url_parts'],
                    'query_params': value['raw_query_params'],
                    'anomaly_score': value['anomaly_score'],
                    'is_anomaly': bool(value['is_anomaly'])
                }]
            )
            
        except Exception as e:
            logging.error(f"ClickHouse insert failed: {str(e)}")
            
    def close(self):
        if self.client:
            logging.info("Closing ClickHouse connection")

def kafka_sink_example():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///jars/flink-sql-connector-kafka-3.0.1-1.18.jar")

    kafka_broker = os.environ["KAFKA_BROKER"]
    kafka_topic = os.environ["KAFKA_TOPIC"]

    clickhouse_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse-service")
    clickhouse_port = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
    clickhouse_db = os.environ.get("CLICKHOUSE_DB", "default") 
    clickhouse_table = os.environ.get("CLICKHOUSE_TABLE", "log_anomalies")

    print(f"Connecting to Kafka broker: {kafka_broker}, topic: {kafka_topic}")
    print(f"ClickHouse target: {clickhouse_host}:{clickhouse_port}/{clickhouse_db}.{clickhouse_table}")

    t_env = StreamTableEnvironment.create(env)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_broker) \
        .set_topics(kafka_topic) \
        .set_group_id("flink_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    print("Kafka source created successfully")

    ds = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    parsed_stream = ds.map(ParseJson(), output_type=row_type)

    print("Parsed stream created successfully")
    
    table = t_env.from_data_stream(parsed_stream).alias(
        "level", "ts", "caller", "msg", "remote_ip", "latency", "host",
        "http_method", "request_uri", "http_version", "response_status", 
        "response_size", "referrer", "request_body", "request_time",
        "user_agent", "request_id"
    )
    
    t_env.create_temporary_system_function("encode_method", encode_method)
    t_env.create_temporary_system_function("encode_protocol", encode_protocol)
    t_env.create_temporary_system_function("encode_status", encode_status)
    t_env.create_temporary_system_function("extract_device", extract_device)
    t_env.create_temporary_system_function("map_device", map_device)
    t_env.create_temporary_system_function("encode_country", encode_country)
    t_env.create_temporary_system_function("parse_latency", parse_latency)
    
    processed_table = table.select(
        col("response_size"),
        encode_method(col("http_method")).alias("encoded_http_method"),
        encode_protocol(col("http_version")).alias("encoded_http_version"),
        encode_status(col("response_status")).alias("encoded_response_status"),
        map_device(extract_device(col("user_agent"))).alias("encoded_device"),
        encode_country(col("remote_ip")).alias("encoded_country"),
        parse_latency(col("latency")).alias("parsed_latency"),
        split_request(col("request_uri")).alias("request_parts"),
        split_url(col("request_uri")).alias("url_parts"),
        extract_query_array(col("request_body")).alias("query_params"),
    )
    
    print("Processed table created successfully") 

    result_stream = t_env.to_data_stream(processed_table)
    
    combined_stream = result_stream.map(
        CombineProcessedWithRaw(),
        output_type=Types.MAP(Types.STRING(), Types.PRIMITIVE())
    )
    
    combined_stream.add_sink(
        ClickHouseSink(
            host=clickhouse_host,
            port=clickhouse_port,
            database=clickhouse_db,
            table=clickhouse_table
        )
    )
    
    # Keep the print for debugging
    combined_stream.print("Combined Data for ClickHouse")
    
    env.execute("Kafka to ClickHouse Job")

if __name__ == "__main__":
    kafka_sink_example()
