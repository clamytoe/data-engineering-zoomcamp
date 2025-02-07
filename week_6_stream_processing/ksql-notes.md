# ksqlDB for Stream Processing

```bash

                  ===========================================
                  =       _              _ ____  ____       =
                  =      | | _____  __ _| |  _ \| __ )      =
                  =      | |/ / __|/ _` | | | | |  _ \      =
                  =      |   <\__ \ (_| | | |_| | |_) |     =
                  =      |_|\_\___/\__, |_|____/|____/      =
                  =                   |_|                   =
                  =        The Database purpose-built       =
                  =        for stream processing apps       =
                  ===========================================

Copyright 2017-2022 Confluent Inc.

CLI v7.3.2, Server v7.3.2 located at http://ksqldb-server:8088
Server Status: RUNNING

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

ksql>
```

## Tech Stack

- Confluent Kafka (with Zookeeper)
- Confluent Schema Registry
- Confluent REST Proxy
- [ksqlDB](https://ksqldb.io/)
- [kpow](https://docs.kpow.io/ce/)
- [Conduktor Platform](https://www.conduktor.io/explorer/)
- Docker, docker-compose

## Up and Running

### Architecture Notice

- Take notice that all docker images pulled use `linux/arm64` platform architecture, as I'm using Apple Sillicon (M1).
- Switch that to `linux/amd64` from `linux/arm64`, if you're using an Intel/AMD CPU (x86_64 / amd64) architecture

### Developer Setup

**1.** Fire up the Confluent Platform and the UIs with:

```bash
docker-compose up -d
```

**2.** Start up with the ksqlDB CLI

```bash
docker exec -it cp-ksqldb-cli ksql http://ksqldb-server:8088
```

You should be getting into this console:

```bash
                  ===========================================
                  =       _              _ ____  ____       =
                  =      | | _____  __ _| |  _ \| __ )      =
                  =      | |/ / __|/ _` | | | | |  _ \      =
                  =      |   <\__ \ (_| | | |_| | |_) |     =
                  =      |_|\_\___/\__, |_|____/|____/      =
                  =                   |_|                   =
                  =        The Database purpose-built       =
                  =        for stream processing apps       =
                  ===========================================

Copyright 2017-2022 Confluent Inc.

CLI v7.3.2, Server v7.3.2 located at http://ksqldb-server:8088
Server Status: RUNNING

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

ksql>
```

**3.** Pick one of the following 2x tools as a Web UI for Kafka Administration

**3.1.** `kpow` Web UI:

```bash
open http://localhost:3000/
```

**3.2.** `Conduktor Platform` Web UI:

```bash
open http://localhost:8080
```

Credentials:

- `username:` admin@conduktor.io
- `password:` admin

## TODO

- [x] Set up a Kafka Cluster with ZooKeeper
- [x] Set up Confluent Schema Registry
- [x] Set up Confluent Rest Proxy
- [x] Set up ksqlDB Server and CLI
- [x] Set up `kpow` Web UI
- [x] Set up `Conduktor Platform` Web UI
- [x] Set up a `UI for Kafka` Web UI
- [ ] Switch Kafka to use KRaft instead
