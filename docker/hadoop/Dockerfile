# Phase 2 container for the Pig and MapReduce pipelines.
#
# Starts from the same pinned Hive image (it already ships Hadoop 3.3.6 and Java 8), then adds:
#   * a full JDK 8 (the Hive image has only a JRE, and MapReduce jobs must be compiled with javac)
#   * Apache Pig 0.18.0, the first Pig release built for Hadoop 3
# Both run in Hadoop local mode: one JVM, files read from and written to the mounted /runs folder.
# Every base image and download is pinned by digest or checksum, so every laptop builds the same thing.

FROM eclipse-temurin:8-jdk@sha256:238468a6b1e411e775bf264e639189a938eb0e887f16a2dea027c854c616b4f1 AS jdk

FROM apache/hive:4.0.1@sha256:5194161ef50b80875f937dff04936df047cbe894e9727ab8f0606349345b1bd3

USER root
COPY --from=jdk /opt/java/openjdk /opt/jdk8
ADD --checksum=sha256:6845adb936a3c3bcc71451765953cae4103e410ba2f6c5a47cc9becade0a434a \
    https://dlcdn.apache.org/pig/pig-0.18.0/pig-0.18.0.tar.gz /tmp/pig.tar.gz
RUN tar -xzf /tmp/pig.tar.gz -C /opt \
 && mv /opt/pig-0.18.0 /opt/pig \
 && rm /tmp/pig.tar.gz \
 && mkdir -p /opt/pig/logs \
 && chown -R hive:hive /opt/pig

ENV JAVA_HOME=/opt/jdk8 \
    PIG_HOME=/opt/pig \
    PIG_CLASSPATH=/opt/hadoop/etc/hadoop \
    PATH=/opt/jdk8/bin:/opt/pig/bin:/opt/hadoop/bin:${PATH}

USER hive
WORKDIR /runs
# No server to start: the pipelines run `pig` and `hadoop jar` with docker compose exec.
ENTRYPOINT []
CMD ["sleep", "infinity"]
