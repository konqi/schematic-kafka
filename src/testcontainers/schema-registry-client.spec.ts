import { CompatibilityMode, SchemaRegistryClient, SchemaRegistryError, SchemaType } from "../schema-registry-client"
import { StartedDockerComposeEnvironment } from "testcontainers"
import { up } from "./helper"

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

describe("SchemaRegistryClient AVRO (Black-Box Tests)", () => {
  // registryPort = 8081
  let client: SchemaRegistryClient
  const subject = "TEST_TOPIC-value"
  let testSchemaId: number

  beforeAll(async () => {
    client = new SchemaRegistryClient({
      baseUrl: `http://localhost:${schemaRegistryPort}`,
    })

    const result = await client.registerSchema(subject, { schemaType: SchemaType.AVRO, schema: `{"type":"string"}` })
    testSchemaId = result.id
    expect(testSchemaId).toBeGreaterThan(0)
  })

  afterAll(async () => {
    // soft delete
    const softDeletedIds = await client.deleteSubject(subject)

    // perma delete
    const permanentDeletedIds = await client.deleteSubject(subject, true)
    expect(softDeletedIds).toEqual(permanentDeletedIds)
  })

  it("should get schema type", async () => {
    const result = await client.getSchemaTypes()
    expect(result).toEqual(["JSON", "PROTOBUF", "AVRO"])
  })

  it("schema by id", async () => {
    const schema = await client.getSchemaById(testSchemaId)
    expect(schema.schema.length).toBeGreaterThan(0)
  })

  it("list subjects", async () => {
    const availableSubjects = await client.listSubjects()
    expect(availableSubjects).toEqual([subject])
  })

  it("versions by id", async () => {
    const versions = await client.listVersionsForId(testSchemaId)
    expect(versions).toHaveLength(1)
  })

  it("versions by subject & get schema for subject and version", async () => {
    const versions = await client.listVersionsForSubject(subject)
    expect(versions).toHaveLength(1)

    const schema = await client.getSchemaForSubjectAndVersion(subject, versions[0])
    expect(schema.id).toEqual(testSchemaId)
    expect(schema.version).toEqual(versions[0])
    expect(schema.subject).toEqual(subject)
    expect(schema.schema?.length).toBeGreaterThan(0)

    const rawSchema = await client.getRawSchemaForSubjectAndVersion(subject, versions[0])
    expect(rawSchema).toEqual(schema.schema)
  })

  it("should get the latest schema version for a subject", async () => {
    const schema = await client.getLatestVersionForSubject(subject)
    expect(schema.id).toEqual(testSchemaId)
    expect(schema.version).toBeGreaterThan(0)
    expect(schema.subject).toEqual(subject)
    expect(schema.schema?.length).toBeGreaterThan(0)
  })

  it("can check a schema", async () => {
    const result = await client.checkSchema(subject, { schemaType: SchemaType.AVRO, schema: `{"type":"string"}` })
    // this should return the id for the existing schema
    expect(result.id).toEqual(testSchemaId)
  })

  it("returns error for unknown schema during check", async () => {
    const result = client.checkSchema("unknown_subject", {
      schemaType: SchemaType.AVRO,
      schema: `{"type":"string"}`,
    })
    await expect(result).rejects.toThrowError(new SchemaRegistryError(404, "Subject 'unknown_subject' not found."))
  })

  it("fetches default schema compatibility mode", async () => {
    const defaultCompatibility = await client.getConfig()
    expect(defaultCompatibility).toEqual(CompatibilityMode.BACKWARD)
  })
  it("sets default schema compatibility mode", async () => {
    const defaultCompatibility = await client.getConfig()

    await client.setConfig(CompatibilityMode.FORWARD)
    expect(await client.getConfig()).toEqual(CompatibilityMode.FORWARD)

    await client.setConfig(defaultCompatibility)
  })
  it("set and fetch subject schema compatibility mode", async () => {
    const defaultCompatibility = await client.getConfig()
    await client.setSubjectConfig(subject, CompatibilityMode.FULL_TRANSITIVE)
    expect(await client.getSubjectConfig(subject)).toEqual(CompatibilityMode.FULL_TRANSITIVE)
    await client.setSubjectConfig(subject, defaultCompatibility)
    expect(await client.getSubjectConfig(subject)).toEqual(defaultCompatibility)
  })
  it("successfully checks schema compatibility", async () => {
    // technically this is not a change
    const compatibility = await client.testCompatibility(subject, "latest", {
      schemaType: SchemaType.AVRO,
      schema: `{"type":"string"}`,
    })
    expect(compatibility).toEqual(true)
  })
  it("returns incompatible for schema check that is breaking change", async () => {
    const compatibility = await client.testCompatibility(subject, "latest", {
      schemaType: SchemaType.AVRO,
      schema: `{"type":"boolean"}`,
    })
    expect(compatibility).toEqual(false)
  })
  it("rejects upload of incompatible schema", async () => {
    const request = client.registerSchema(subject, {
      schemaType: SchemaType.AVRO,
      schema: `{"type":"boolean"}`,
    })
    const rejection = await expect(request).rejects.toThrow(
      /^Schema registry error: code: 409 - Schema being registered is incompatible with an earlier schema for subject/
    )
  })
})

describe("SchemaRegistryClient PROTOBUF (Black-Box Tests)", () => {
  let client: SchemaRegistryClient
  const subject = "TEST_PROTOBUF_TOPIC-value"
  let testSchemaId: number
  const testSchema = `syntax = "proto3";
  package com.acme;
  
  
  message MyRecord {
    string f1 = 1;
  }`

  beforeAll(async () => {
    client = new SchemaRegistryClient({
      baseUrl: `http://localhost:${schemaRegistryPort}`,
    })

    const result = await client.registerSchema(subject, {
      schemaType: SchemaType.PROTOBUF,
      schema: testSchema,
    })
    testSchemaId = result.id
    expect(testSchemaId).toBeGreaterThan(0)
  })

  afterAll(async () => {
    // soft delete
    const softDeletedIds = await client.deleteSubject(subject)

    // perma delete
    const permanentDeletedIds = await client.deleteSubject(subject, true)
    expect(softDeletedIds).toEqual(permanentDeletedIds)
  })

  it("should return schemaType for schemas that don't use AVRO", async () => {
    const result = await client.getLatestVersionForSubject(subject)
    expect(result.schemaType).toEqual(SchemaType.PROTOBUF)
  })

  it("has no clue about the schemaType", async () => {
    const result = await client.getSchemaById(testSchemaId)
    expect(result.schemaType).toEqual(SchemaType.PROTOBUF)
  })
})
