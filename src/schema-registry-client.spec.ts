import * as nock from "nock"

import {
  CompatibilityMode,
  SchemaApiClientConfiguration,
  SchemaRegistryClient,
  SchemaRegistryError,
  SchemaType,
} from "./schema-registry-client"

describe("SchemaRegistryClient (Integration Tests)", () => {
  const schema = { type: "string" }
  const schemaPayload = { schemaType: "AVRO", schema: JSON.stringify(schema) }
  const apiClientOptions: SchemaApiClientConfiguration = {
    baseUrl: "http://test.com/",
  }

  const schemaApi = new SchemaRegistryClient(apiClientOptions)

  afterEach(() => {
    nock.cleanAll()
  })

  describe("registerSchema", () => {
    it("reject if post request fails", async () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").post("/subjects/topic/versions").replyWithError(requestError)

      const result = schemaApi.registerSchema("topic", schemaPayload)
      await expect(result).rejects.toEqual(requestError)
    })

    it("reject if post request returns with not 200", async () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").post("/subjects/topic/versions").reply(500, mockError)

      const result = schemaApi.registerSchema("topic", schemaPayload)
      await expect(result).rejects.toThrowError(new SchemaRegistryError(mockError.error_code, mockError.message))
    })

    it("resolve with schema id if post request returns with 200", async () => {
      nock("http://test.com").post("/subjects/topic/versions").reply(200, { id: 1 })

      const result = schemaApi.registerSchema("topic", schemaPayload)
      await expect(result).resolves.toEqual({ id: 1 })
    })
  })

  describe("getSchemaById", () => {
    it("reject if get request fails", async () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/schemas/ids/1").replyWithError(requestError)

      const result = schemaApi.getSchemaById(1)
      await expect(result).rejects.toBeInstanceOf(Error)
      await expect(result).rejects.toEqual(requestError)
    })

    it("reject if get request returns with not 200", async () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").get("/schemas/ids/1").reply(500, mockError)

      const result = schemaApi.getSchemaById(1)
      await expect(result).rejects.toThrowError(new SchemaRegistryError(mockError.error_code, mockError.message))
    })

    it("resolve with schema if get request returns with 200", async () => {
      const mockResponse = { id: 1, schema }
      nock("http://test.com").get("/schemas/ids/1").reply(200, mockResponse)

      const result = schemaApi.getSchemaById(1)
      await expect(result).resolves.toEqual(mockResponse)
    })
  })

  describe("getSchemaTypes", () => {
    it("resolves with schema types", async () => {
      const mockResponse = ["AVRO", "PROTOBUF", "JSON"]
      nock("http://test.com").get("/schemas/types").reply(200, mockResponse)

      const result = schemaApi.getSchemaTypes()
      await expect(result).resolves.toEqual(mockResponse)
    })
  })

  describe("listSubjects", () => {
    it("resolves with available subjects", async () => {
      const mockResponse = ["subject1", "subject2"]
      nock("http://test.com").get("/subjects").reply(200, mockResponse)

      const result = schemaApi.listSubjects()
      await expect(result).resolves.toEqual(mockResponse)
    })
  })

  describe("listVersionsForId", () => {
    it("resolves with existing schema versions for a schemaId", async () => {
      const schemaId = 1
      const mockResponse = [{ subject: "test-subject1", version: 1 }]
      nock("http://test.com").get(`/schemas/ids/${schemaId}/versions`).reply(200, mockResponse)

      const result = schemaApi.listVersionsForId(schemaId)
      await expect(result).resolves.toEqual(mockResponse)
    })
  })

  describe("listVersionsForSubject", () => {
    it("resolves with available verions fiven a subject name", async () => {
      const subject = "subject"
      const mockResponse = [1, 2, 3, 4]
      nock("http://test.com").get(`/subjects/${subject}/versions`).reply(200, mockResponse)

      const result = schemaApi.listVersionsForSubject(subject)
      await expect(result).resolves.toEqual(mockResponse)
    })
  })

  describe("deleteSubject", () => {
    it("deletes a subject", async () => {
      const subject = "subject"
      const mockResponse = [1, 2, 3, 4]
      nock("http://test.com").delete(`/subjects/${subject}`).reply(200, mockResponse)

      const result = schemaApi.deleteSubject(subject)
      await expect(result).resolves.toEqual(mockResponse)
    })

    it("permanently deletes a subject", async () => {
      const subject = "subject"
      const mockResponse = [1, 2, 3, 4]
      nock("http://test.com").delete(`/subjects/${subject}?permanent=true`).reply(200, mockResponse)

      const result = schemaApi.deleteSubject(subject, true)
      await expect(result).resolves.toEqual(mockResponse)
    })
  })

  describe("getSchemaForSubjectAndVersion", () => {
    it("returns specific version of schema registered under subject", async () => {
      const subject = "subject"
      const version = 1
      const mockResponse = {
        name: "test",
        version: 1,
        schema: '{"type": "string"}',
      }
      nock("http://test.com").get(`/subjects/${subject}/versions/${version}`).reply(200, mockResponse)

      const result = schemaApi.getSchemaForSubjectAndVersion(subject, version)
      await expect(result).resolves.toEqual(mockResponse)
    })

    it("returns latest version of schema registered under subject", async () => {
      const subject = "subject"
      const mockResponse = {
        name: "test",
        version: 1,
        schema: '{"type": "string"}',
      }
      nock("http://test.com").get(`/subjects/${subject}/versions/latest`).reply(200, mockResponse)

      const result = schemaApi.getSchemaForSubjectAndVersion(subject)
      await expect(result).resolves.toEqual(mockResponse)
    })

    it("getLatestVersionForSubject is an alias for the above", async () => {
      const subject = "subject"
      const mockResponse = {
        name: "test",
        version: 1,
        schema: '{"type": "string"}',
      }
      nock("http://test.com").get(`/subjects/${subject}/versions/latest`).reply(200, mockResponse)

      const result = schemaApi.getLatestVersionForSubject(subject)
      await expect(result).resolves.toEqual(mockResponse)
    })
  })

  describe("getRawSchemaForSubjectAndVersion", () => {
    it("returns the schema for a given subject and version", async () => {
      const subject = "subject"
      const version = 1
      const mockResponse = { type: "string" }

      nock("http://test.com").get(`/subjects/${subject}/versions/${version}/schema`).reply(200, mockResponse)

      const result = schemaApi.getRawSchemaForSubjectAndVersion(subject, version)
      await expect(result).resolves.toEqual(JSON.stringify(mockResponse))
    })
  })

  describe("checkSchema", () => {
    const fakeSchema = {
      subject: "test",
      id: 1,
      version: 3,
      schema:
        '{"type": "record","name": "test","fields":[{"type": "string","name": "field1"},{"type": "int","name": "field2"}]}',
    }

    it("schema already exists", async () => {
      const subject = "subject"
      nock("http://test.com").post(`/subjects/${subject}`).reply(200, fakeSchema)

      const result = schemaApi.checkSchema(subject, { schema: fakeSchema.schema })

      await expect(result).resolves.toEqual(fakeSchema)
    })

    it("schema does not exist", async () => {
      const subject = "subject"
      nock("http://test.com").post(`/subjects/${subject}`).reply(404, { error_code: 404, message: "Nope" })

      const result = schemaApi.checkSchema(subject, { schema: fakeSchema.schema })

      await expect(result).rejects.toThrowError(new SchemaRegistryError(404, "Nope"))
    })

    it("responds with empty response body", async () => {
      const subject = "subject"
      nock("http://test.com").post(`/subjects/${subject}`).reply(404)

      const result = schemaApi.checkSchema(subject, { schema: fakeSchema.schema })

      await expect(result).rejects.toThrowError(new Error("Invalid schema registry response"))
    })

    it("responds with invalid response body", async () => {
      const subject = "subject"
      nock("http://test.com").post(`/subjects/${subject}`).reply(404, "not json")

      const result = schemaApi.checkSchema(subject, { schema: fakeSchema.schema })

      await expect(result).rejects.toThrowError(new SyntaxError("Unexpected token o in JSON at position 1"))
    })
  })

  describe("getLatestVersionForSubject", () => {
    it("reject if first get request fails", async () => {
      const requestError = new Error("ECONNREFUSED")
      nock("http://test.com").get("/subjects/topic/versions/latest").replyWithError(requestError)

      const result = schemaApi.getLatestVersionForSubject("topic")
      await expect(result).rejects.toEqual(requestError)
    })

    it("reject if first get request returns with not 200", async () => {
      const mockError = { error_code: 1, message: "failed request" }
      nock("http://test.com").get("/subjects/topic/versions/latest").reply(500, mockError)

      const result = schemaApi.getLatestVersionForSubject("topic")
      await expect(result).rejects.toThrowError(new SchemaRegistryError(mockError.error_code, mockError.message))
    })

    it("resolve with schema and id if both get requests return with 200", async () => {
      nock("http://test.com").get("/subjects/topic/versions/latest").reply(200, { id: 1, schema })

      const result = schemaApi.getLatestVersionForSubject("topic")
      await expect(result).resolves.toEqual({ schema, id: 1 })
    })
  })

  describe("compatibility and config", () => {
    it("should set the schema registry's default compatibilty mode", async () => {
      nock("http://test.com")
        .put("/config")
        .reply(200, (_uri, body: string) => ({
          compatibility: JSON.parse(body).compatibility,
        }))

      const result = schemaApi.setConfig(CompatibilityMode.FULL)
      await expect(result).resolves.toEqual(CompatibilityMode.FULL)
    })
    it("should get the schema registry's default compatibility mode", async () => {
      nock("http://test.com").get("/config").reply(200, {
        compatibilityLevel: "BACKWARD",
      })

      const result = schemaApi.getConfig()
      await expect(result).resolves.toEqual(CompatibilityMode.BACKWARD)
    })

    it("should set the compatibility mode for a subject", async () => {
      nock("http://test.com")
        .put("/config/subject")
        .reply(200, (_uri, body: string) => ({
          compatibility: JSON.parse(body).compatibility,
        }))

      const result = schemaApi.setSubjectConfig("subject", CompatibilityMode.FULL)
      await expect(result).resolves.toEqual(CompatibilityMode.FULL)
    })

    it("should get the compatibility mode for a subject", async () => {
      nock("http://test.com").get("/config/subject").reply(200, {
        compatibilityLevel: "BACKWARD",
      })

      const result = schemaApi.getSubjectConfig("subject")
      await expect(result).resolves.toEqual(CompatibilityMode.BACKWARD)
    })

    it.each`
      isCompatible
      ${true}
      ${false}
    `(
      "should signal (in)compatibility, when schema changes are (not) allowed by compatibity mode",
      async ({ isCompatible }) => {
        const subject = "custom-subject"
        const version = "latest"
        nock("http://test.com").post(`/compatibility/subjects/${subject}/versions/${version}`).reply(200, {
          is_compatible: isCompatible,
        })
        const fakeSchema = {
          subject: "test",
          id: 1,
          version: 3,
          schema:
            '{"type": "record","name": "test","fields":[{"type": "string","name": "field1"},{"type": "int","name": "field2"}]}',
        }

        const result = await schemaApi.testCompatibility(subject, version, fakeSchema)
        expect(result).toEqual(isCompatible)
      }
    )
  })
})
