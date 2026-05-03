FROM apache/spark:3.5.3

USER root

RUN pip install --no-cache-dir \
    pandas \
    numpy \
    psycopg2-binary \
    kagglehub

RUN curl -fSL https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    -o /opt/spark/jars/postgresql-42.7.4.jar

USER spark
