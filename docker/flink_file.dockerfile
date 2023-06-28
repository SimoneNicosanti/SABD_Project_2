FROM flink:latest

# python configuration
RUN apt-get update -y 
RUN apt install python3 -y
RUN apt-get update -y
RUN apt-get install python3-pip -y
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install dependencies
RUN pip3 install apache-flink
## Change version or link if needed
RUN curl -o /KafkaConnectorDependencies.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar
RUN pip3 install jproperties