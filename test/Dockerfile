FROM debian:stretch

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    procps \
    curl gnupg \
    build-essential

ADD skroutz-stable.list /etc/apt/sources.list.d/
ADD skroutz-pu.list /etc/apt/sources.list.d/
RUN curl -sSL http://debian.skroutz.gr/skroutz.asc | apt-key add -

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    golang \
    golang-github-confluentinc-confluent-kafka-go-dev \
    golang-github-secmask-go-redisproto-dev \
    golang-github-urfave-cli-dev \
    golang-golang-x-sync-dev

ENV GOPATH /usr/share/gocode
ENV GOTRACEBACK crash

VOLUME ["/rafka"]
WORKDIR "/rafka"

ENTRYPOINT ["test/docker-entrypoint.sh"]
EXPOSE 6380

CMD ["-k", "test/kafka.test.json", "-i", "1"]
