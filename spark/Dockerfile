FROM spark:3.5.1-python3 AS dependencies

USER root

RUN printf 'print("Spark dependencies resolved.")\n' > /tmp/resolve.py

RUN printf '%s\n' \
    '<ivysettings>' \
    '  <settings defaultResolver="central"/>' \
    '  <resolvers>' \
    '    <ibiblio name="central" m2compatible="true" root="https://repo.maven.apache.org/maven2/"/>' \
    '  </resolvers>' \
    '</ivysettings>' \
    > /tmp/ivysettings.xml

RUN set -eu; \
    for attempt in 1 2 3; do \
        if /opt/spark/bin/spark-submit \
            --master "local[1]" \
            --conf spark.jars.ivy=/tmp/ivy \
            --conf spark.jars.ivySettings=/tmp/ivysettings.xml \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
            /tmp/resolve.py; then \
            break; \
        fi; \
        if [ "$attempt" = "3" ]; then exit 1; fi; \
        sleep 5; \
    done; \
    mkdir -p /opt/extra-jars; \
    for jar in /tmp/ivy/jars/*.jar; do \
        name="${jar##*/}"; \
        name="${name#*_}"; \
        if [ ! -f "/opt/spark/jars/$name" ]; then \
            cp "$jar" "/opt/extra-jars/$name"; \
        fi; \
    done

FROM spark:3.5.1-python3

COPY --from=dependencies /opt/extra-jars/ /opt/spark/jars/