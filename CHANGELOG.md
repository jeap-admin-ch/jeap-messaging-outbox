# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

[8.44.0] - 2025-06-16
### Changed
- update jeap-messaging.version from 8.42.0 to 8.44.0 because messaging outbox was not yet in automated process

[8.43.0] - 2025-06-13
### Changed
Extracted jeap-messaging-outbox modules to its own repository
Changelog before: see jeap-messaging
## [8.48.1] - 2025-06-19
### Changed
- update jeap-messaging.version from 8.47.0 to 8.47.1
- Fix bug in message signing verifier, where certificate common and service name were twisted 


## [8.48.0] - 2025-06-18

### Changed
- Update parent from 5.10.1 to 5.10.2
- update jeap-spring-boot-vault-starter.version from 17.37.0 to 17.38.0
- update jeap-messaging.version from 8.46.0 to 8.47.0
- update jeap-crypto.version from 3.22.1 to 3.23.0

## [8.47.0] - 2025-06-18
### Changed
- update jeap-messaging.version from 8.45.0 to 8.46.0
- Overwrite commons-io version (2.11.0) from spring-kafka-test 3.3.6 with 2.19.0 (CVE-2024-47554)
- Overwrite commons-beanutils version (1.9.4) from spring-kafka-test 3.3.6 with 1.11.0 (CVE-2025-48734)


## [8.46.0] - 2025-06-17
### Changed
- update jeap-messaging.version from 8.44.0 to 8.45.0
- Update parent from 5.10.0 to 5.10.1
- Update because to upload (central-publish) didn't work properly
- update jeap-crypto.version from 3.21.0 to 3.22.1


