import { parse, Type as AVSCInstance } from "avsc"
import { Field, Type } from "protobufjs"
import { StartedDockerComposeEnvironment } from "testcontainers"

import { KafkaRegistryHelper } from "../kafka-registry-helper"
import { SchemaType } from "../schema-registry-client"
import { up } from "./helper"

let testcontainers: StartedDockerComposeEnvironment
let schemaRegistryPort: number

beforeAll(async () => {
  // increase timeout to 10 minutes (docker compose from scratch will probably take longer)
  jest.setTimeout(1000 * 60 * 10)

  const env = await up()
  testcontainers = env.testcontainers
  schemaRegistryPort = env.schemaRegistryPort

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
    registry = new KafkaRegistryHelper({ baseUrl: `http://localhost:${schemaRegistryPort}` }).withSchemaHandler(
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
    registry = new KafkaRegistryHelper({ baseUrl: `http://localhost:${schemaRegistryPort}` }).withSchemaHandler(
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
