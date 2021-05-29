![workflow-status](https://github.com/konqi/schematic-kafka/actions/workflows/build-actions.yml/badge.svg) ![test-status](https://github.com/konqi/schematic-kafka/actions/workflows/test-actions.yml/badge.svg) [![Coverage Status](https://coveralls.io/repos/github/konqi/schematic-kafka/badge.svg?branch=main)](https://coveralls.io/github/konqi/schematic-kafka?branch=main)

# schematic-kafka

This package provides a **schema type agnostic** kafka message encoder/decoder. It works fine with whatever protocol you may want to use, but it doesn't take care of this aspect. Have a look at the code sample below to understand what this means.

## Quickstart

### Install

```bash
npm install avsc schematic-kafka
# or
yarn add avsc schematic-kafka
```

### Use

```typescript
import { KafkaRegistryHelper, SchemaType } from "schematic-kafka"
import { parse, Type as AVSCInstance } from "avsc"

// create instance
const registry = new KafkaRegistryHelper({ baseUrl: "https://schemaRegistryHost:8081" })
  // adding a custom schema handler for AVRO
  // as mentioned before, the library doesn't take care of this
  .withSchemaHandler(SchemaType.AVRO, (schema: string) => {
    // if you want to customize your encoder, this is where you'd do it
    const avsc: AVSCInstance = parse(schema)
    return {
      encode: (message: any) => {
        return avsc.toBuffer(message)
      },
      decode: (message: Buffer) => {
        return avsc.fromBuffer(message)
      },
    }
  })

// decode a message from kafka
// AVSC returns parsed json, so decodedMessage is a ready to use object
const decodedMessage = await registry.decode(rawMessageFromKafka)

// encode a message with a schema
// where
// - subject    is the kafka topic plus the (-key, -value) postfix
// - message    the actual message to send (this has to be in whatever format
//              the schema handler defined above expects in the encode-function)
// - schemaType (optional if already registerd, default: AVRO) AVRO/PROTOBUF/JSON
// - schema     (optional if already registerd) serialized schema to be used
// returns      a Buffer that you can send to the kafka broker
const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
```

For more examples, take a look at `src/kafka-registry-helper.testcontainersspec.ts`.

## How this library works

This is how a kafka message looks like when you send or receive it.

```
[ 1 byte  | 0      | 0 indicates this message is schema encoded ]
[ 4 bytes | number | schema id                                  ]
[ n bytes | msg    | protocol encoded message                   ]
```

The first byte being a zero tells us that the following four bytes contain the schema id. With this schema id we can request the schema type (AVRO, PROTOBUF or JSON) and schema (serialized representation of the schema for the corresponding schema type) from the schema registry.

This library can decodes whole kafka message header and then calls the appropriate decoder that you provide with the schema as argument.

## Documentation

The package exports the following things

- `KafkaRegistryHelper` is the wrapper that handles encoding/decoding of messages, it also registeres your payload schemas if the registry doesn't know about them yet
- `SchemaRegistryClient` is the low level api client that communicates with the schema-registry
- `SchemaType` Is an enum containing the available schema types (AVRO, PROTOBUF, JSON)

`KafkaRegistryHelper` applies a cache for some of the schema registry requests of `SchemaRegistryClient` to go easy on the network traffic and load.

### Client SSL authentication

This library uses node's http/https request. As such you can provide an Agent to modify your requests.

```typescript
import { Agent } from "https"

const agent = new Agent({
  key: readFileSync("./client.key"),
  cert: readFileSync("./client.cert"),
})
new KafkaRegistryHelper({ baseUrl: "https://schemaRegistryHost:8081", agent })
...
```

### Basic authentication

```typescript
new KafkaRegistryHelper({ baseUrl: "https://schemaRegistryHost:8081", username: "username", password: "password })

// OR

new KafkaRegistryHelper({ baseUrl: "https://username:password@schemaRegistryHost:8081" })
```

## feature x

TODO - document things hint: Most methods have jsdoc comments on them. Have a look.

# TODO

A few things that need to be done:

- ✅ increase test coverage to at least 85%
- ✅ stretch goal: test coverage of at least 95%
- ✅ Add missing jsdoc information
- ✅ fix caching mechanism for non-primitive values
- ✅ create working sample/testcontainers-test for PROTOBUF messages
- ✅ create a working example for how to use this package with kafka.js
- create example for how to configure nestjs kafka-module serializer/deserializer
- create working example for use of referenced schemas (AVRO/PROTOBUF)

# Motivation

I created this library out of necessity. I wanted to use [confluent-schema-registry](https://github.com/kafkajs/confluent-schema-registry). However I wasn't able to get a client-certificate TLS-secured connection to a schema-registry working. All I got was an SSL error with code 42 in an underlying layer. I wasn't able to configure mappersmith, which handles http connections. I'm aware that a recent merge should fix that, but the head version from the repo didn't work for me either. I also didn't like the list dependencies 😉. I had a look at [avro-schema-registry](https://github.com/bencebalogh/avro-schema-registry) which can't use SSL authentication at all. Being a nice little library, I decided to fork and make the changes and, well, things got a little out of hand... Now everything is pure typescript and the package handles just the schema-registry aspect.

# Dependencies

The module has just the tslib as dependency.
