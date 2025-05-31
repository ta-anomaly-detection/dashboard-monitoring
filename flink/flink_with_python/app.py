import requests
import json
import logging

from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.datastream.functions import MapFunction
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col


from config import (
    KAFKA_BROKER, 
    KAFKA_TOPIC, 
    CLICKHOUSE_HOST, 
    CLICKHOUSE_PORT, 
    CLICKHOUSE_DB, 
    CLICKHOUSE_TABLE, 
    CLICKHOUSE_USER, 
    CLICKHOUSE_PASSWORD,
    print_configuration
)
from udfs import (
    register_udfs,
    encode_method,
    encode_protocol,
    encode_status,
    extract_device,
    map_device,
    parse_latency,
    split_request,
    split_url,
    encode_country
)

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
    field_names=["level", "ts", "caller", "msg", "remote_ip", "latency", "host", "http_method", "request_uri", "http_version",
     "response_status", "response_size", "referrer", "request_body", "request_time", "user_agent", "request_id"],
    field_types=[Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.STRING()]
)

class CombineProcessedWithRaw(MapFunction):
    def map(self, row):
        try:
            return {
                'ts': row[10] or "",  
                'remote_ip': row[11] or "",
                'latency_us': row[6] or 0.0, 
                'host': row[12] or "",
                'http_method': row[13] or "",
                'encoded_http_method': row[1] or 0,
                'request_uri': row[14] or "",
                'url_path': row[8][0] if row[8] else "",
                'url_query': row[8][1] if row[8] else "",
                'http_version': row[15] or "",
                'encoded_http_version': row[2] or 0,
                'response_status': row[16] or 0,
                'encoded_status': row[3] or 0,
                'response_size': row[0] or 0,
                'user_agent': row[17] or "",
                'device_family': row[18] or "",
                'encoded_device': row[4] or 0,
                'country': row[19] or "",
                'encoded_country': row[5] or 0,
                'referrer': row[20] or "",
                'request_id': row[21] or "",
                'msg': row[22] or "",
                'level': row[23] or ""
            }
        except Exception as e:
            logging.error(f"Failed to combine data: {e}")
            return {
                'ts': "", 'remote_ip': "", 'latency_us': 0.0, 'host': "", 
                'http_method': "", 'encoded_http_method': 0, 'request_uri': "", 
                'url_path': "", 'url_query': "", 'http_version': "", 
                'encoded_http_version': 0, 'response_status': 0, 'encoded_status': 0, 
                'response_size': 0, 'user_agent': "", 'device_family': "", 
                'encoded_device': 0, 'country': "", 'encoded_country': 0,
                'referrer': "", 'request_id': "", 'msg': "", 'level': ""
            }
        
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

def kafka_sink_example():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///jars/flink-sql-connector-kafka-3.0.1-1.18.jar")
    env.add_jars("file:///jars/flink-connector-jdbc-3.1.2-1.17.jar")
    env.add_jars("file:///jars/clickhouse-jdbc-0.4.6-all.jar") 
    
    print_configuration()

    t_env = StreamTableEnvironment.create(env)

    register_udfs(t_env)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(KAFKA_TOPIC) \
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
    
    processed_table = table.select(
        col("response_size"),
        encode_method(col("http_method")).alias("encoded_http_method"),
        encode_protocol(col("http_version")).alias("encoded_http_version"),
        encode_status(col("response_status")).alias("encoded_status"),
        map_device(extract_device(col("user_agent"))).alias("encoded_device"),
        encode_country(col("remote_ip")).alias("encoded_country"),
        parse_latency(col("latency")).alias("latency_us"),
        split_request(col("request_uri")).alias("request_parts"),
        split_url(col("request_uri")).alias("url_parts"),
        col("ts"),
        col("remote_ip"),
        col("host"),
        col("http_method"),
        col("request_uri"),
        col("http_version"),
        col("response_status"),
        col("user_agent"),
        extract_device(col("user_agent")).alias("device_family"),
        col("remote_ip").alias("country"),  # Replace with actual country extraction if available
        col("referrer"),
        col("request_id"),
        col("msg"),
        col("level")
    )
    
    print("Processed table created successfully") 

    result_stream = t_env.to_data_stream(processed_table)

    combined_stream = result_stream.map(
        CombineProcessedWithRaw(),
        output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())
    )

    # Define the SQL for inserting into ClickHouse
    sql = f"""
    INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
        ts, remote_ip, latency_us, host, http_method, encoded_http_method,
        request_uri, url_path, url_query, http_version, 
        encoded_http_version, response_status, encoded_status, response_size,
        user_agent, device_family, encoded_device, country, encoded_country,
        referrer, request_id, msg, level
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    clickhouse_row_type = Types.ROW_NAMED(
        field_names=[
            "ts", "remote_ip", "latency_us", "host", "http_method", 
            "encoded_http_method", "request_uri", "url_path", "url_query", 
            "http_version", "encoded_http_version", "response_status", 
            "encoded_status", "response_size", "user_agent", 
            "device_family", "encoded_device", "country", 
            "encoded_country", "referrer", "request_id", "msg", "level"
        ],
        field_types=[
            Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING(),
            Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), 
            Types.STRING(), Types.INT(), Types.INT(),
            Types.INT(), Types.INT(), Types.STRING(),
            Types.STRING(), Types.INT(), Types.STRING(),
            Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()
        ]
    )

    connection_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder()\
        .with_url(f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")\
        .with_driver_name("com.clickhouse.jdbc.ClickHouseDriver")\
        .with_user_name(CLICKHOUSE_USER)\
        .with_password(CLICKHOUSE_PASSWORD)\
        .build()
    
    execution_options = JdbcExecutionOptions.Builder() \
        .with_batch_interval_ms(1000) \
        .with_batch_size(1000) \
        .with_max_retries(3) \
        .build()

    jdbc_sink = JdbcSink.sink(
        sql,
        type_info=clickhouse_row_type,
        jdbc_execution_options=execution_options,
        jdbc_connection_options=connection_options
    )
    
    combined_stream.add_sink(jdbc_sink)

    combined_stream.print("Data for ClickHouse")
    
    env.execute("Kafka to ClickHouse JDBC Job")

if __name__ == "__main__":
    kafka_sink_example()
