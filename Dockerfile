FROM golang:1.18 as builder

WORKDIR /srv
COPY . /srv

RUN apt-get update \
    && apt-get install -y librdkafka-dev golint \
    && make

FROM debian:bullseye-slim

RUN apt-get update \
      && apt-get install -y librdkafka1 librdkafka-dev ca-certificates \
      && rm -rf /var/lib/apt/lists/* \
      && groupadd -r rafka \
      && useradd --no-log-init -s /bin/bash -r -g rafka rafka

COPY --chown=rafka:rafka --from=builder /srv/rafka /srv/
COPY --chown=rafka:rafka --from=builder /srv/librdkafka.json.aws /srv/librdkafka.json

USER rafka
WORKDIR /srv

ENTRYPOINT [ "/srv/rafka", "-c", "/srv/librdkafka.json" ]
