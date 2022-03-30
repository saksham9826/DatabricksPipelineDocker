# set base image (host OS)
FROM gcr.io/spark-operator/spark-py:v3.0.0

# set the working directory in the container
WORKDIR /

COPY / .

COPY delta-core_2.12-0.8.0.jar /opt/spark/jars

COPY scalaSpark-assembly-1.0.jar /opt/spark/jars
USER root
RUN apt-get -y install librdkafka-dev

RUN python3 --version
# install dependencies
RUN pip3 install -r requirements.txt

RUN export PYSPARK_PYTHON=python3

RUN pip3 show python-schema-registry-client
# copy the content of the local src directory to the working directory
COPY src/ .
