FROM postgres:9.4

COPY 01-create-schemas.sql /docker-entrypoint-initdb.d/01-create-schema.sql
COPY 02-init-tables.sql /docker-entrypoint-initdb.d/02-init-tables.sql
COPY 03-create-role-and-permissions.sql /docker-entrypoint-initdb.d/03-create-role-and-permissions.sql

ENV POSTGRES_USER=db_username
ENV POSTGRES_PASSWORD=db_password
ENV POSTGRES_DB=db_name

ENV PGDATA /var/lib/postgresql/data2
RUN mkdir -p "$PGDATA" && chown -R postgres:postgres "$PGDATA" && chmod 777 "$PGDATA"

RUN /docker-entrypoint.sh postgres --version

# Don't use the entrypoint script from the upstream Dockerfile
ENTRYPOINT []

USER postgres
CMD postgres