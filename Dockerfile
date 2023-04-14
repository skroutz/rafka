FROM golang:1.11 as builder

RUN apt-get update \
    && apt-get install -y \
        ca-certificates \
        libgnutls30 \
        librdkafka-dev \
    && git config --global --add safe.directory /app/rafka \
    && rm -rf /var/lib/apt/lists/*


COPY . /app/rafka
WORKDIR /app/rafka

RUN make build

FROM harbor.skroutz.gr/skroutz/debian:buster

RUN apt-get update \
    && apt-get install -y \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r rafka \
    && useradd --no-log-init --shell /bin/bash -r -g rafka rafka

USER rafka

COPY --chown=rafka:rafka --from=builder /app/rafka/rafka /srv/rafka/rafka
COPY --chown=rafka:rafka ./librdkafka.json.sample /srv/rafka/librdkafka.json

EXPOSE 6380

ENTRYPOINT [ "/srv/rafka/rafka", "-c", "/srv/rafka/librdkafka.json"]
