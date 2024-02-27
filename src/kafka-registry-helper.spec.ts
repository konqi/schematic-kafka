import * as nock from "nock"
import { parse, Type as AVSCInstance, Type } from "avsc"

import { KafkaRegistryHelper } from "./kafka-registry-helper"
import { SchemaRegistryError, SchemaType } from "./schema-registry-client"

describe("KafkaRegistryHelper with AVRO", () => {
  const host = "http://localhost:8081"
  const subject = "REGISTRY_TEST_SUBJECT"
  let registry: KafkaRegistryHelper
  const message = { hello: "world" }
  const type = Type.forValue(message)
  const schema = type.toString()

  beforeAll(() => {
    // Create registry helper instance and attach avro schema handler
    registry = new KafkaRegistryHelper({ baseUrl: host }).withSchemaHandler(SchemaType.AVRO, (schema: string) => {
      const avsc: AVSCInstance = parse(schema) // could add all kinds of configurations here
      return {
        encode: (message: any) => {
          return avsc.toBuffer(message)
        },
        decode: (message: Buffer) => {
          return avsc.fromBuffer(message)
        },
      }
    })
  })

  afterEach(() => {
    registry.schemaRegistryClient.cacher.clear()
    nock.cleanAll()
  })

  it("passes through messages without schema preamble", async () => {
    const decodeResult = await registry.decode(Buffer.from("🦆"))
    expect(decodeResult).toEqual(Buffer.from("🦆"))
  })

  it("encodes and decodes AVRO message", async () => {
    const schemaId = 1

    nock(host).post(`/subjects/${subject}`).once().reply(404, { error_code: 404, message: "no" })
    nock(host)
      .post(`/subjects/${subject}/versions`)
      .once()
      .reply(200, (_uri: string, request: string) => {
        return { id: schemaId, ...JSON.parse(request) }
      })
    nock(host).get(`/schemas/ids/${schemaId}`).once().reply(200, { schema: schema })

    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    const decodeResult = await registry.decode(encodeResult)
    expect(decodeResult).toEqual(message)
  })

  it("decodes message and returns possible subject and version", async () => {
    const schemaId = 42

    nock(host).post(`/subjects/${subject}`).once().reply(404, { error_code: 404, message: "no" })
    nock(host)
      .post(`/subjects/${subject}/versions`)
      .once()
      .reply(200, (_uri: string, request: string) => {
        return { id: schemaId, ...JSON.parse(request) }
      })
    nock(host).get(`/schemas/ids/${schemaId}`).once().reply(200, { schema: schema })
    nock(host)
      .get(`/schemas/ids/${schemaId}/versions`)
      .once()
      .reply(200, [{ subject: "🦆", version: schemaId }])

    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    // try decode with subject and version
    const decodeResult = await registry.decodeWithSubjectAndVersionInformation(encodeResult)
    expect(decodeResult.message).toEqual(message)
    expect(decodeResult.subjects).toHaveLength(1)
    expect(decodeResult.subjects!![0].subject).toEqual("🦆")
    expect(decodeResult.subjects!![0].version).toEqual(42)
  })

  it("schema registry return an error other than 404", async () => {
    nock(host).post(`/subjects/${subject}`).once().reply(403, "⚠️")

    const result = registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    await expect(result).rejects.toThrow(SyntaxError)
  })

  it("use schema from registry / schema not provided for encodeForSubject", async () => {
    const schemaId = 1

    nock(host).post(`/subjects/${subject}`).once().reply(404, { error_code: 404, message: "don't know this schema" })
    nock(host).get(`/subjects/${subject}/versions/latest`).once().reply(200, { schema, id: schemaId })
    nock(host).get(`/schemas/ids/${schemaId}`).once().reply(200, { schema, id: schemaId })

    const encoded = await registry.encodeForSubject(subject, message)
    const decoded = await registry.decode(encoded)
    expect(decoded).toEqual(message)
  })

  it("schema not provided and not available in registry", async () => {
    nock(host).post(`/subjects/${subject}`).once().reply(404, { error_code: 404, message: "don't know this schema" })
    nock(host)
      .get(`/subjects/${subject}/versions/latest`)
      .once()
      .reply(404, { error_code: 404, message: "don't know this schema" })

    const result = registry.encodeForSubject(subject, message)
    expect(result).rejects.toThrow(new SchemaRegistryError(404, "don't know this schema"))
  })

  it("missing schema handler for encodeForSubject", async () => {
    const result = registry.encodeForSubject(subject, message, SchemaType.PROTOBUF)
    expect(result).rejects.toThrowError(/No.*PROTOBUF/)
  })

  it("mismatch between given schemaType and schemaType in registry", async () => {
    const schemaId = 1
    nock(host)
      .get(`/subjects/${subject}/versions/latest`)
      .once()
      .reply(200, { schema, id: schemaId, schemaType: SchemaType.PROTOBUF })

    const result = registry.encodeForSubject(subject, message)
    await expect(result).rejects.toThrow(/Mismatch/)
  })

  it("encodes for schema id", async () => {
    const schemaId = 1

    nock(host).get(`/schemas/ids/${schemaId}`).once().reply(200, { schema, id: schemaId })

    const encoded = await registry.encodeForId(schemaId, message)
    const decoded = await registry.decode(encoded)

    expect(decoded).toEqual(message)
  })

  it("should detect schemaType mismatch between registry and argument", async () => {
    const schemaId = 1

    nock(host)
      .get(`/schemas/ids/${schemaId}`)
      .once()
      .reply(200, { schema, id: schemaId, schemaType: SchemaType.PROTOBUF })

    const result = registry.encodeForId(schemaId, message)

    await expect(result).rejects.toThrow(/Mismatch/)
  })

  it("cannot encode for schema id without schemaType handler", async () => {
    const schemaId = 1

    nock(host).get(`/schemas/ids/${schemaId}`).once().reply(200, { schema, id: schemaId })

    const result = registry.encodeForId(schemaId, message, SchemaType.PROTOBUF)

    await expect(result).rejects.toThrow(/No.*handler.*PROTOBUF/)
  })

  it("passes through payloads that are not schema encoded", async () => {
    const result = await registry.decode(Buffer.from("Hello World!"))
    expect(result.toString()).toEqual("Hello World!")
  })
})

describe("KafkaRegistryHelper with PROTOBUF", () => {
  const host = "http://localhost:8081"
  const subject = "REGISTRY_TEST_SUBJECT_PROTOBUF"
  let registry: KafkaRegistryHelper
  const message = { hello: "world" }
  const schema = "ADD PROTOBUF SCHEMA HERE"

  beforeAll(() => {
    // Create registry helper instance and attach avro schema handler
    registry = new KafkaRegistryHelper({ baseUrl: host }).withSchemaHandler(SchemaType.PROTOBUF, (schema: string) => {
      // TODO: this is where some PROTOBUF magic would happen
      return {
        encode: (message: any) => {
          // TODO: this is really not PROTOBUF
          return Buffer.from(JSON.stringify(message))
        },
        decode: (message: Buffer) => {
          // TODO: more like PROTOBUG
          return JSON.parse(message.toString())
        },
      }
    })
  })

  it("encodes and decodes PROTOBUF message", async () => {
    const schemaId = 1

    nock(host).post(`/subjects/${subject}`).once().reply(404, { error_code: 404, message: "no" })
    nock(host)
      .post(`/subjects/${subject}/versions`)
      .once()
      .reply(200, (_uri: string, request: string) => {
        return { id: schemaId, ...JSON.parse(request) }
      })
    nock(host).get(`/schemas/ids/${schemaId}`).once().reply(200, { schema: schema, schemaType: SchemaType.PROTOBUF })

    // nothing is cached here
    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.PROTOBUF, schema)
    const decodeResult = await registry.decode(encodeResult)
    expect(decodeResult).toEqual(message)
  })
})

describe("caching logic", () => {
  const host = "http://localhost:8081"
  const subject = "REGISTRY_TEST_SUBJECT"
  let registry: KafkaRegistryHelper
  const message = { hello: "world" }
  const type = Type.forValue(message)
  const schema = type.toString()

  beforeAll(() => {
    // Create registry helper instance and attach avro schema handler
    registry = new KafkaRegistryHelper({ baseUrl: host }).withSchemaHandler(SchemaType.AVRO, (schema: string) => {
      const avsc: AVSCInstance = parse(schema) // could add all kinds of configurations here
      return {
        encode: (message: any) => {
          return avsc.toBuffer(message)
        },
        decode: (message: Buffer) => {
          return avsc.fromBuffer(message)
        },
      }
    })
  })

  afterEach(() => {
    registry.schemaRegistryClient.cacher.clear()
    nock.cleanAll()
  })

  it("uses caching", async () => {
    const schemaId = 1

    nock(host).post(`/subjects/${subject}`).once().reply(404, { error_code: 404, message: "no" })
    nock(host)
      .post(`/subjects/${subject}/versions`)
      .once()
      .reply(200, (_uri: string, request: string) => {
        return { id: schemaId, ...JSON.parse(request) }
      })
    nock(host).get(`/schemas/ids/${schemaId}`).once().reply(200, { schema: schema })

    // nothing is cached here
    const encodeResult = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    const decodeResult = await registry.decode(encodeResult)
    expect(decodeResult).toEqual(message)

    // now that the schema is registered the registry would return the schema for check
    nock(host).post(`/subjects/${subject}`).once().reply(200, { schema, id: schemaId })
    const encodeResultB = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    const decodeResultB = await registry.decode(encodeResultB)
    expect(decodeResultB).toEqual(message)

    // lastly, everything should be cached 🤙
    const encodeResultC = await registry.encodeForSubject(subject, message, SchemaType.AVRO, schema)
    const decodeResultC = await registry.decode(encodeResultC)
    expect(decodeResultC).toEqual(message)
  })
})
