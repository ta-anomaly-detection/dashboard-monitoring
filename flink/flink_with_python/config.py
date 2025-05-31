import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

# ClickHouse configuration
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "default")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "web_server_logs")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "admin_password")

def print_configuration():
    """Print the current configuration settings."""
    print(f"Connecting to Kafka broker: {KAFKA_BROKER}, topic: {KAFKA_TOPIC}")
    print(f"ClickHouse target: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")
