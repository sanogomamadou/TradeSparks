FROM apache/flink:1.18

USER root
RUN wget -O /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar

RUN apt-get update && apt-get install -y python3 python3-pip
COPY moving_average.py /job/moving_average.py
RUN pip3 install apache-flink==1.18.0

CMD ["python3", "/job/moving_average.py"]
