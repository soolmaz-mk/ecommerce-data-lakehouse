FROM apache/iceberg-rest-fixture:1.10.1

USER root

ARG POSTGRES_JDBC_VERSION=42.7.13
ARG POSTGRES_JDBC_SHA1=a6e1bd21b412d6ffb3df23cd13d507bc2cc9e37d
RUN curl -fSL -o /usr/lib/iceberg-rest/postgresql.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" && \
    echo "${POSTGRES_JDBC_SHA1}  /usr/lib/iceberg-rest/postgresql.jar" | sha1sum -c - && \
    chown iceberg:iceberg /usr/lib/iceberg-rest/postgresql.jar

USER iceberg:iceberg

CMD ["java", "-cp", "/usr/lib/iceberg-rest/postgresql.jar:/usr/lib/iceberg-rest/iceberg-rest-adapter.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
