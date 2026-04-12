FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir pyyaml

RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.10.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar \
 && curl -fsSL -o /opt/spark/jars/clickhouse-jdbc-0.9.8-all.jar \
        https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.8/clickhouse-jdbc-0.9.8-all.jar


RUN mkdir -p /home/spark/.ivy2 \
 && chown -R spark:spark /home/spark

ENV HOME=/home/spark

USER spark
