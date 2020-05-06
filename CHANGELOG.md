# Changelog

Breaking changes are prefixed with a "[BREAKING]" label.


## master (unreleased)


## 0.6.1 (2019-10-09)

This is a maintenance release with no changes.

## 0.6.0 (2019-10-08)

This is a release refactoring the internal data structures of Rafka as detailed in [Rafka
Rethinking](https://github.com/skroutz/rafka/blob/master/docs/designs/design-rafka-rethinking.rst)
design doc.

### Changed

- [BREAKING] Drop support for multiple Consumers per Client. From now on, only a [single Consumer can be
  associated with a Client instance](https://github.com/skroutz/rafka/blob/master/docs/designs/design-rafka-rethinking.rst#redefine-rafka-scope)

- [INTERNAL] The ConsumerManager module as well as the respective functionality for managing
  Consumers has been [completely dropped from the
  source](https://github.com/skroutz/rafka/blob/master/docs/designs/design-rafka-rethinking.rst#drop-redundant-functionality).
  Now, the handling of Consumers is split between the Server and the respective Client instances.


## 0.5.0 (2019-08-29)

### Added

- Add support for the MONITOR command [[#80](https://github.com/skroutz/rafka/pull/80)]

### Changed

- [BREAKING] Drop support for librdkafka 0.11.4 and before. librdkafka 0.11.5 or later
  is now required [[#76](https://github.com/skroutz/rafka/pull/76)]

- The consumer will now strip "unset" offsets (aka those of `OffsetInvalid` type) from the logging
  output [[#79](https://github.com/skroutz/rafka/pull/79)]

## 0.4.0 (2019-05-24)

### Fixed

- With librdkafka 0.11.6 and 1.0.0, consumers could block indefinitely in BLPOP,
  resulting in rafka being unable to shutdown [[#77](https://github.com/skroutz/rafka/pull/77)]

### Added

- "Unsupported command" errors now contain the actual command that was
  attempted by the client [[fa45217](https://github.com/skroutz/rafka/commit/fa45217c8c451591a009dc1398a6d0813916d5bb)]

### Changed

- If there are no messages to be consumed, BLPOP returns a "null array" instead
  of a "null string", adhering to the Redis protocol. This is an internal change
  that shouldn't affect clients [[adf3650](https://github.com/skroutz/rafka/commit/adf365095ee006a5a0fe31ea633c9038f5f2ec70)]

- Connection write/flush errors are not logged anymore [[686af22](https://github.com/skroutz/rafka/commit/686af22073877159849d716659e6db2206962d8a)]


## 0.3.0 (2019-05-14)

### Added

- The server now logs errors during writing or flushing a response to the
  client [[71aacf5](https://github.com/skroutz/rafka/commit/71aacf59b12d31d5beee905c26b6c1f6d3715a59)]

### Changed

- Incoming message size limit is bumped from 64kB to 32MB [[75fa7ef](https://github.com/skroutz/rafka/commit/75fa7ef023ec55d3c60b1e08e72f0afd127cd92a)]

### Fixed

- Properly return _all_ parse errors to clients [[db39b5f](https://github.com/skroutz/rafka/commit/db39b5f978e39e9bd91017cba94b312a8014dca6)]


## 0.2.0 (2018-10-01)

### Added

- Support for listing topics [[#66](https://github.com/skroutz/rafka/pull/66)]
- Flag for displaying the rafka version (`--version/-v`) [[c650dd0](https://github.com/skroutz/rafka/commit/c650dd063d3468e80e3b7d96549285ffa1d7c951)]

### Changed

- Shutdown process is more robust with less downtime for producers [[#68](https://github.com/skroutz/rafka/pull/68)]


## 0.1.0 (2018-09-24)

### Fixed

- Ignore non-critical "Poll GroupCoordinator" errors [[#69](https://github.com/skroutz/rafka/pull/69)]

### Changed

- [BREAKING] `--kafka/-k` flag is renamed to `--config/-c` [[9be4ea8](https://github.com/skroutz/rafka/commit/9be4ea84d2e7ddf8b33d90e0f6489dd07335dfef)]


## 0.0.16 (2018-06-07)

### Fixed

- Consumers failed to start if librdkafka configuration provided from
  clients contained numeric values [[118c36a](https://github.com/skroutz/rafka/commit/118c36af1969b1df81ce0d29f1a36696f94e8a2a)]


## 0.0.15 (2018-06-05)

This is a maintenance release with no changes.


## 0.0.14 (2018-06-05)

### Added

- Accept configuration from clients [[#40](https://github.com/skroutz/rafka/issues/40)]

### Fixed

- Consumer could hang indefinitely when closing [[#59](https://github.com/skroutz/rafka/issues/59)]


## 0.0.13 (2018-06-05)

### Added

- Offset commit results are now visible in the logs [[c73dae](https://github.com/skroutz/rafka/commit/c73dae044be7903d6b11109cc5cc366d61d98228)]


## 0.0.12 (2018-05-22)

### Changed

- Use librdkafka auto commit and offset store functionality


## 0.0.11 (2018-05-18)

### Changed

- Depend on librdkafka 0.11.4 and confluent-kafka-go 0.11.4
