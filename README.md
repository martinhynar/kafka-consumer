# kafka-consumer
Consume events from Kafka

## Running
+ Have Kafka installed on localhost, listening at default port 9092
+ Create topic named `experimental` which is default topic used by this consumer client
```
kafka-topics \
  --zookeeper localhost:2181 \
  --create \
  --topic experimental \
  --partitions 1 \
  --replication-factor 1
```
+ Run `go run kafka-consumer.go`

The client will subscribe to `experimental` topic and will try to find topics after January 1st, 2018. Then it will seek to the position. This means, all records after this moment will be read. Practically, it will seek to offset 0, but not directly but by calling `OffsetsForTimes` function.
