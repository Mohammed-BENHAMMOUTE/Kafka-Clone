[![progress-banner](https://backend.codecrafters.io/progress/kafka/a7660190-054a-402b-bb8e-55cf30b29f80)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

# Build Your Own Kafka in Java

This repository contains a Java implementation of a simplified Kafka clone, built as part of the ["Build Your Own Kafka" Challenge](https://codecrafters.io/challenges/kafka) on CodeCrafters.

## About The Project

The goal of this challenge is to build a toy Kafka clone that can handle `APIVersions` and `Fetch` API requests. Along the way, you'll learn about:

* The Kafka wire protocol for message encoding and decoding.
* Handling network protocols and TCP sockets.
* Implementing event loops.

**Note**: If you're viewing this repo on GitHub, head over to [codecrafters.io](https://codecrafters.io) to try the challenge.

## Getting Started

Follow these instructions to get a copy of the project up and running on your local machine.

### Prerequisites

Make sure you have Maven installed on your system.

* **mvn**

  ```sh
  mvn --version
  ```

### Running the Broker

1. Execute the `your_program.sh` script to start the Kafka broker.

   ```sh
   ./your_program.sh
   ```

   The broker is implemented in `src/main/java/Main.java`.

2. To submit your solution to CodeCrafters, commit your changes and push to the `master` branch.

   ```sh
   git commit -am "Implement feature"
   git push origin master
   ```

   Test output will be streamed to your terminal.
