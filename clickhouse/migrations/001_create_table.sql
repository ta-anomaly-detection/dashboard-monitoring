CREATE TABLE web_server_logs (
    ip String,
    time DateTime,
    method String,
    response_time Float32,
    url String,
    param String,
    protocol String,
    response_code UInt16,
    response_byte UInt32,
    user_agent String,
    ingestion_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (time);