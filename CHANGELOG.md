# Changelog

Breaking changes are prefixed with a "[BREAKING]" label.

## master (unreleased)

### Added

- Support for listing topics [[#66](https://github.com/skroutz/rafka/pull/66)]







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
