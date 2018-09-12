FROM debian:stretch

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    procps \
    curl gnupg \
    build-essential

ADD skroutz-stable.list /etc/apt/sources.list.d/
ADD skroutz-pu.list /etc/apt/sources.list.d/
RUN curl -sSL http://debian.skroutz.gr/skroutz.asc | apt-key add -

# TODO: install librdkafka-dev from main dist when it's backported
RUN apt-get update && \
    apt-get install -y librdkafka-dev -t stretch-skroutz-proposed-updates && \
    apt-get install -y --no-install-recommends \
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

CMD ["-c", "test/librdkafka.test.json"]
