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

from utils import clean_ts

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
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

            if "log" in outer_data:
                data = json.loads(outer_data["log"])
            else:
                data = outer_data

            return Row(
                data.get("ip"),
                clean_ts(data.get("time")),
                data.get("method"),
                data.get("responseTime"),
                data.get("url"),
                data.get("params"),
                data.get("protocol"),
                int(data.get("responseCode", 0)),
                int(data.get("responseByte", 0)),
                data.get("userAgent"),
            )
        except Exception as e:
            logging.error(f"Failed to parse JSON: {e} | Raw message: {value}")
            logging.error(f"Message preview: {value[:200]}...")
            return Row(*[None] * 16)


row_type = Types.ROW_NAMED(
    field_names=["ip", "time", "method", "responseTime", "url",
                 "params", "protocol", "responseCode", "responseByte", "userAgent"],
    field_types=[Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(
    ), Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING()]
)


class CombineProcessedWithRaw(MapFunction):
    def map(self, row):
        try:
            logging.info(f"Processing row: {row}")

            # Now the indices match exactly with the processed_table order
            return Row(
                row[0] or "",                     # ts
                row[1] or "",                     # remote_ip
                float(row[2]) if row[2] is not None else 0.0,  # latency_us
                row[3] or "",                     # host
                row[4] or "",                     # http_method
                # encoded_http_method
                int(row[5]) if row[5] is not None else 0,
                row[6] or "",                     # request_uri
                row[7][0] if row[7] and len(row[7]) > 0 else "",  # url_path
                row[7][1] if row[7] and len(row[7]) > 1 else "",  # url_query
                row[8] or "",                     # http_version
                # encoded_http_version
                int(row[9]) if row[9] is not None else 0,
                # response_status
                int(row[10]) if row[10] is not None else 0,
                int(row[11]) if row[11] is not None else 0,    # encoded_status
                int(row[12]) if row[12] is not None else 0,    # response_size
                row[13] or "",                    # user_agent
                row[14] or "",                    # device_family
                int(row[15]) if row[15] is not None else 0,    # encoded_device
                row[16] or "",                    # country
                # encoded_country
                int(row[17]) if row[17] is not None else 0,
                row[18] or "",                    # referrer
                row[19] or "",                    # request_id
                row[20] or "",                    # msg
                row[21] or ""                     # level
            )
        except Exception as e:
            logging.error(f"Failed to combine data: {e}")
            logging.error(f"Row content: {row}")
            logging.error(
                f"Row length: {len(row) if hasattr(row, '__len__') else 'N/A'}")
            # Return a valid Row with default values
            return Row(
                "", "", 0.0, "", "", 0, "", "", "", "",
                0, 0, 0, 0, "", "", 0, "", 0, "", "", "", ""
            )


def kafka_sink_example():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///jars/flink-sql-connector-kafka-3.0.1-1.18.jar")
    env.add_jars("file:///jars/flink-connector-jdbc-3.1.2-1.17.jar")
    env.add_jars("file:///jars/clickhouse-jdbc-0.4.6-all.jar")
    # env.add_jars("file:///jars/flink-metrics-prometheus-1.18.1.jar")

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

    ds = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    parsed_stream = ds.map(ParseJson(), output_type=row_type)

    table = t_env.from_data_stream(parsed_stream).alias(
        "ip",
        "time",
        "method",
        "responseTime",
        "url",
        "params",
        "protocol",
        "responseCode",
        "responseByte",
        "userAgent"
    )

    processed_table = table.select(
        col("ip"),
        col("time"),
        col("method"),
        parse_latency(col("responseTime")).alias("response_time"),
        col("url"),
        col("params"),
        col("protocol"),
        col("responseCode"),
        col("responseByte"),
        col("userAgent"),
    )

    result_stream = t_env.to_data_stream(processed_table)
    
    # debug
    result_stream.print("Processed Stream")

    clickhouse_row_type = Types.ROW_NAMED(
        field_names=[
            "ip", "time", "method", "response_time", "url",
            "params", "protocol", "response_code", "response_byte", "user_agent"
        ],
        field_types=[
            Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(),
            Types.STRING(), Types.STRING(), Types.STRING(),
            Types.INT(), Types.INT(), Types.STRING()
        ]
    )

    # combined_stream = result_stream.map(
    #     CombineProcessedWithRaw(),
    #     output_type=clickhouse_row_type
    # )

    sql = f"""
    INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
        ip, time, method, response_time, url, params, protocol,
        response_code, response_byte, user_agent
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

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

    result_stream.add_sink(jdbc_sink)

    result_stream.print("Data for ClickHouse")

    env.execute("Kafka to ClickHouse JDBC Job")


if __name__ == "__main__":
    kafka_sink_example()
