# MiniDFS test cluster for clients that cannot run the Maven harness. Arguments
# are the feature flags used by the Rust tests (security, kms, ha, ec, ...).
#
# Run with host networking: datanodes, MiniKdc and MiniKMS bind ephemeral ports
# on localhost. Client files (core-site.xml, hdfs.keytab, kdc/*/krb5.conf,
# delegation_token) are written to /minidfs/target/test.

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q dependency:copy-dependencies -DoutputDirectory=lib
COPY src src
RUN mvn -q compile

FROM eclipse-temurin:17-jre
WORKDIR /minidfs
COPY --from=build /build/lib lib
COPY --from=build /build/target/classes classes
ENV HADOOP_KEYSTORE_PASSWORD=kmspassword
# core-site.xml is written once the cluster is ready.
HEALTHCHECK --interval=2s --retries=150 CMD grep -q '</configuration>' target/test/core-site.xml
# docker stop sends SIGTERM, which shuts the cluster down. Remove the config from a
# previous run so the healthcheck waits for this one.
ENTRYPOINT ["sh", "-c", "rm -f target/test/core-site.xml && exec java -cp 'classes:lib/*' main.Main \"$@\"", "minidfs"]
