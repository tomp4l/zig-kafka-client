# Zig Kafka Client

A native Kafka client built from scratch in Zig 0.16. This project builds on `std.Io` as a basis for async operations.

**Note:** This project is experimental and for learning and not meant as a production library.

## Current

- JSON definitions from [the official repo](https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message) -> code gen
- Single broker connection async message handling
- Basic producer interface (sync single topic/partition)

## Next

- Basic consumer interface
- Tighten up existing functionality

_This project is licensed under the MIT License. Certain files imported from Apache Kafka are licensed under the Apache License 2.0. See the LICENSE-APACHE and NOTICE files for details._
