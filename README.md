# Zig Kafka Client

A native Kafka client built from scratch in Zig 0.16. This project builds on `std.Io` as a basis for async operations.

**Note:** This project is a foundation and a starting point. The core architecture (JSON code generation, flexible tag buffer parsing, and concurrent request/response routing) is implemented.

## Current

- JSON definitions from [the official repo](https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message) -> code gen
- Single broker connection async message handling

## Next

- Create a usable client interface around the raw broker connections.
- Generate more message types and get a working consumer / producer set up.

_This project is licensed under the MIT License. Certain files imported from Apache Kafka are licensed under the Apache License 2.0. See the LICENSE-APACHE and NOTICE files for details._
