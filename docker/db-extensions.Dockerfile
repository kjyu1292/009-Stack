# docker/graphdb.Dockerfile
FROM postgres:16

RUN apt-get update && apt-get install -y \
    build-essential git \
    libreadline-dev zlib1g-dev \
    flex bison postgresql-server-dev-16 \
    && rm -rf /var/lib/apt/lists/*

# pgvector
RUN git clone --branch v0.8.4 https://github.com/pgvector/pgvector.git /tmp/pgvector \
    && cd /tmp/pgvector && make && make install \
    && rm -rf /tmp/pgvector

# Apache AGE
RUN git clone --branch release/PG16/1.6.0 https://github.com/apache/age.git /tmp/age \
    && cd /tmp/age && make && make install \
    && rm -rf /tmp/age
