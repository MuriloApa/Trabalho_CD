FROM python:latest

RUN pip install pyspark

WORKDIR /app

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/default-java

# Baixando o driver JDBC do PostgreSQL para conexão com o banco
RUN mkdir -p /opt/drivers && \
    curl -o /opt/drivers/postgresql-42.2.5.jar https://jdbc.postgresql.org/download/postgresql-42.2.5.jar

# Adicionando o driver JDBC ao classpath
ENV SPARK_CLASSPATH=/opt/drivers/postgresql-42.2.5.jar
ENV CLASSPATH=$CLASSPATH:/opt/drivers/postgresql-42.2.5.jar


COPY teste_conexao.py /app


CMD ["python", "teste_conexao.py"]
