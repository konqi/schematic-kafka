import { parse, Type as AVSCInstance } from "avsc"
import { parse as protobufParse } from "protobufjs"
import { StartedDockerComposeEnvironment } from "testcontainers"

import { KafkaRegistryHelper } from "../kafka-registry-helper"
import { SchemaType } from "../schema-registry-client"
import { findTypes, up } from "./helper"

import { dirname } from "path"
import { readFileSync } from "fs"

let testcontainers: StartedDockerComposeEnvironment
let schemaRegistryPort: number

beforeAll(async () => {
  const env = await up()
  testcontainers = env.testcontainers
  schemaRegistryPort = env.schemaRegistryPort
}, 1000 * 60 * 10 /* increase timeout to 10 minutes (docker compose from scratch will probably take longer) */)

afterAll(async () => {
  await testcontainers?.down()
}, 60000)

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
  const message = {
    isCerealSoup: "maybe",
  }
  const schema = readFileSync(`${dirname(__filename)}/KafkaTestMessage.proto`).toString()

  let registry: KafkaRegistryHelper

  beforeAll(() => {
    registry = new KafkaRegistryHelper({ baseUrl: `http://localhost:${schemaRegistryPort}` }).withSchemaHandler(
      SchemaType.PROTOBUF,
      (schema: string) => {
        // construct protobuf parser from schema type
        const root = protobufParse(schema).root
        const types = findTypes(root)
        if (types.length !== 1) throw Error("There can only be one type")
        const protobufType = root.lookupType(types[0])

        return {
          encode: (message: any) => {
            return protobufType.encode(message).finish() as Buffer
          },
          decode: (message: Buffer) => {
            return protobufType.decode(message)
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
    const decodeResult = await registry.decode(encodeResult)
    expect(decodeResult).toEqual(message)
  })
})
