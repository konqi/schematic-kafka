import { parse, Type as AVSCInstance } from "avsc"
import { Field, Type } from "protobufjs"
import { DockerComposeEnvironment, StartedDockerComposeEnvironment, Wait } from "testcontainers"

import { KafkaRegistryHelper } from "../kafka-registry-helper"
import { SchemaType } from "../schema-registry-client"
import { findPort } from "./helper"

let testcontainers: StartedDockerComposeEnvironment
let registryPort: number

beforeAll(async () => {
  const TAG = "5.5.4"

  // increase timeout to 10 minutes (docker compose from scratch will probably take longer)
  jest.setTimeout(1000 * 60 * 10)

  const kafkaPort = await findPort()

  testcontainers = await new DockerComposeEnvironment(".", "docker-compose.yml")
    .withEnv("TAG", TAG)
    .withEnv("KAFKA_PORT", `${kafkaPort}`)
    .withWaitStrategy("zookeeper_1", Wait.forLogMessage("binding to port"))
    .withWaitStrategy("broker_1", Wait.forLogMessage("Awaiting socket connections"))
    .withStartupTimeout(1000 * 60 * 3)
    .up()

  registryPort = testcontainers.getContainer("schema-registry_1").getMappedPort(8081)

  jest.setTimeout(15000)
})

afterAll(async () => {
  jest.setTimeout(60000)
  await testcontainers?.down()
})

describe("KafkaRegistryHelper (AVRO)", () => {
  const subject = "REGISTRY_TEST_SUBJECT"
  const message = { hello: "world" }
  const type = AVSCInstance.forValue(message)
  const jsonSchema = type.toJSON()
  jsonSchema["name"] = "test"
  const schema = JSON.stringify(jsonSchema)
  let registry: KafkaRegistryHelper

  beforeAll(() => {
    registry = new KafkaRegistryHelper({ baseUrl: `http://localhost:${registryPort}` }).withSchemaHandler(
      SchemaType.AVRO,
      (schema: string) => {
        const avsc: AVSCInstance = parse(schema) // could add all kinds of configurations here
        return {
          encode: (message: string) => {
            return avsc.toBuffer(message)
          },
          decode: (message: Buffer) => {
            return avsc.fromBuffer(message)
          },
        }
      }
    )
  })

  it("encodes and decodes AVRO message", async () => {
    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    const decodeResult = await registry.decode(encodeResult)
    expect(decodeResult).toEqual(message)
  })
})

describe("KafkaRegistryHelper (PROTOBUF)", () => {
  const subject = "REGISTRY_TEST_PROTOBUF_SUBJECT-value"
  const schema = `syntax = "proto3";
  package com.example;

  message KafkaTestMessage {
      string is_cereal_soup = 1;
  }`
  const message = JSON.stringify({
    isCerealSoup: "yes",
  })
  const protobufType = new Type("KafkaTestMessage").add(new Field("isCerealSoup", 1, "string"))
  let registry: KafkaRegistryHelper

  beforeAll(() => {
    registry = new KafkaRegistryHelper({ baseUrl: `http://localhost:${registryPort}` }).withSchemaHandler(
      SchemaType.PROTOBUF,
      (schema: string) => {
        // TODO this is where the schema would be used to construct the protobuf type
        return {
          encode: (message: string) => {
            return protobufType.encode(protobufType.fromObject(JSON.parse(message))).finish() as Buffer
          },
          decode: (message: Buffer) => {
            return JSON.stringify(protobufType.decode(message).toJSON())
          },
        }
      }
    )
  })

  afterEach(async () => {
    await registry.schemaRegistryClient.deleteSubject(subject)
    await registry.schemaRegistryClient.deleteSubject(subject, true)
  })

  it("encodes and decodes message", async () => {
    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.PROTOBUF, schema)
    console.log(encodeResult)
    const decodeResult = await registry.decode(encodeResult)
    console.log(decodeResult)
    expect(decodeResult.toString()).toEqual(message)
  })
})
