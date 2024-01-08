# README TFM
This is a document to explain deeply how was the deployment of an open source IoT cloud deployed.

First of all, I created a project in Python so I could carry on with some scripts that explain how I managed to integrate Kafka, the tool that is going to be used as the middleware (Publish/Subscribe highly scalable messaging system, between my IoT devices and the frontend), into my project (by building a client application which will consume and produce messages from an Apache Kafka cluster). 

Kafka will allow me to administrate consumers and producers using a key-value pair messaging. More info below:  
Kafka uses the abstraction of a distributed log that consists of partitions. Splitting a log into partitions allows to scale-out the system.
_"Keys are used to determine the partition within a log to which a message get's appended to. While the value is the actual payload of the message. The examples are actually not very "good" with this regard; usually you would have a complex type as value (like a tuple-type or a JSON or similar) and you would extract one field as key: https://stackoverflow.com/questions/40872520/whats-the-purpose-of-kafkas-key-value-pair-based-messaging"_

After I successfully created my project in Python, I had to install several libraries in my environment (env):  
1. confluent-kafka: This Python client provides a high-level producer, consumer, and AdminClient that are compatible with Kafka brokers (version 0.8 or later), Confluent Cloud, and Confluent Platform: https://docs.confluent.io/kafka-clients/python/current/overview.html
2. confluent-kafka [avro]
3. CoAPTthon3
Tutorial: https://developer.confluent.io/get-started/python/
