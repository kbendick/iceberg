FROM jupyter/all-spark-notebook:latest

LABEL maintainer="Kyle Bendickson <no asf email and probably I shouldnt be listed as maintainer"

# This part isn't strictly necessary because it's set up in the jupyter all-spark-notebook
# ENV SPARK_HOME "/usr/local/spark"
ENV SPARK_JAR_HOME $SPARK_HOME/jars

ENV iceberg_version 0.9.1
ENV maven_iceberg_spark_3_all_url "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark3-runtime/${iceberg_version}/iceberg-spark3-runtime-${iceberg_version}.jar"

# Need to switch back to user root as the jupyter base switches to $NB_UID
USER root

RUN fix-permissions $SPARK_JAR_HOME

RUN set -e && \
  curl "${maven_iceberg_spark_3_all_url}" \
    --output "${SPARK_JAR_HOME}"/iceberg-spark3-runtime-"${iceberg_version}".jar

# Need to set a warehouse location that $NB_UID can access.
# This should really be mounted from the docker-compose
# USER $NB_UID

RUN set -e && \
  mkdir -p /usr/local/warehouse && \
  fix-permissions /usr/local/warehouse


# Need to obtain the randomly selected port for the jupyter UI and then go to localhost:<port from logs> and enter in the token from the logs from this as well
# CMD ["jupyter notebook list"]


