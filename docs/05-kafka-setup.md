

steps to setup 1 zookeeper and 1 kafka broker on a single machine

1. Download the latest version of kafka from the official website
2. Extract the tar file

```bash
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-2.8.0.tgz
```

3. start zookeeper

```bash
cd kafka_2.13-3.7.0
bin/zookeeper-server-start.sh config/zookeeper.properties
```

4. start kafka broker

```bash
bin/kafka-server-start.sh config/server.properties
```

5. create a topic

```bash
bin/kafka-topics.sh --create --topic invoices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

bin/kafka-topics.sh --create --topic lp_notifications --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1


```


