import requests
import json
import logging
import time
from datetime import datetime

from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.datastream.functions import MapFunction
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col

# TODO: Need to install packages via poetry
# from logprocessor import LogPreprocessor

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
    parse_latency,
)

from utils import clean_ts

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

class ParseJson(MapFunction):
    def map(self, value: str) -> Row:
        try:
            # Record processing start time
            processing_start_time = datetime.now()
            
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
                data.get("param"),
                data.get("protocol"),
                int(data.get("responseCode", 0)),
                int(data.get("responseByte", 0)),
                data.get("userAgent"),
                processing_start_time.isoformat()  # Add processing start time
            )
        except Exception as e:
            logging.error(f"Failed to parse JSON: {e} | Raw message: {value}")
            logging.error(f"Message preview: {value[:200]}...")
            return Row(*[None] * 11)  # Updated to match new field count


row_type = Types.ROW_NAMED(
    field_names=["ip", "time", "method", "responseTime", "url",
                 "param", "protocol", "responseCode", "responseByte", "userAgent", "processing_start_time"],
    field_types=[Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
                 Types.STRING(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING()]
)

class AddProcessingTime(MapFunction):
    def map(self, row):
        try:
            # Calculate processing time
            processing_end_time = datetime.now()
            processing_start_time_str = row[10]  # processing_start_time is at index 10
            
            if processing_start_time_str:
                processing_start_time = datetime.fromisoformat(processing_start_time_str)
                processing_duration_ms = (processing_end_time - processing_start_time).total_seconds() * 1000
            else:
                processing_duration_ms = 0.0
            
            # Return row with processing time data, excluding the processing_start_time field
            return Row(
                row[0] or "",                     # ip
                row[1] or "",                     # time
                row[2] or "",                     # method
                float(row[3]) if row[3] is not None else 0.0,  # response_time
                row[4] or "",                     # url
                row[5] or "",                     # param
                row[6] or "",                     # protocol
                int(row[7]) if row[7] is not None else 0,      # response_code
                int(row[8]) if row[8] is not None else 0,      # response_byte
                row[9] or "",                     # user_agent
                processing_duration_ms            # flink_processing_time_ms
            )
        except Exception as e:
            logging.error(f"Failed to add processing time: {e}")
            logging.error(f"Row content: {row}")
            # Return a valid Row with default values
            return Row(
                "", "", "", 0.0, "", "", "", 0, 0, "", 0.0
            )


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

    ds = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    parsed_stream = ds.map(ParseJson(), output_type=row_type)

    table = t_env.from_data_stream(parsed_stream).alias(
        "ip",
        "time",
        "method",
        "responseTime",
        "url",
        "param",
        "protocol",
        "responseCode",
        "responseByte",
        "userAgent",
        "processing_start_time"
    )

    processed_table = table.select(
        col("ip"),
        col("time"),
        col("method"),
        parse_latency(col("responseTime")).alias("response_time"),
        col("url"),
        col("param"),
        col("protocol"),
        col("responseCode"),
        col("responseByte"),
        col("userAgent"),
        col("processing_start_time")
    )

    result_stream = t_env.to_data_stream(processed_table)
    
    # debug
    result_stream.print("Processed Stream")

    clickhouse_row_type = Types.ROW_NAMED(
        field_names=[
            "ip", "time", "method", "response_time", "url",
            "param", "protocol", "response_code", "response_byte", "user_agent", "flink_processing_time_ms"
        ],
        field_types=[
            Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(),
            Types.STRING(), Types.STRING(), Types.STRING(),
            Types.INT(), Types.INT(), Types.STRING(), Types.FLOAT()
        ]
    )

    final_stream = result_stream.map(
        AddProcessingTime(),
        output_type=clickhouse_row_type
    )

    sql = f"""
    INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
        ip, time, method, response_time, url, param, protocol,
        response_code, response_byte, user_agent, flink_processing_time_ms
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    final_stream.add_sink(jdbc_sink)

    final_stream.print("Data for ClickHouse")

    env.execute("Kafka to ClickHouse JDBC Job")


if __name__ == "__main__":
    kafka_sink_example()
