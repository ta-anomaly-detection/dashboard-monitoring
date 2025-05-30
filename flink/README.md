## References
[Flink with Python][(https://nightlies.apache.org/flink/flink-docs-master/](https://github.com/afoley587/flink-with-python))

## How to Run
1. Run `docker-compose up -d`
2. Exec into `app` and run `/flink/bin/flink run -py /taskscripts/app.py --jobmanager jobmanager:8081 --target local`
3. Open other terminal, exec into `dummyproducer`, and run `kafka-console-producer.sh --topic flink-topic --bootstrap-server kafka:9092 < /logs.json`
4. Processed data written in `/sink`