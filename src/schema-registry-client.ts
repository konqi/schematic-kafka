import * as httpsRequest from "https"
import * as httpRequest from "http"
import { URL } from "url"

/**
 * Enum of supported schema types / protocols.
 * If this isn't provided the schema registry will assume AVRO.
 */
export enum SchemaType {
  AVRO = "AVRO", // (default)
  PROTOBUF = "PROTOBUF",
  JSON = "JSON",
}

/**
 * Enum of supported schema compatibility modes.
 */
export enum CompatibilityMode {
  BACKWARD = "BACKWARD",
  BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE",
  FORWARD = "FORWARD",
  FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE",
  FULL = "FULL",
  FULL_TRANSITIVE = "FULL_TRANSITIVE",
  NONE = "NONE",
}

/**
 * Custom error to be used by the schema registry client
 */
export class SchemaRegistryError extends Error {
  errorCode: number

  constructor(errorCode: number, message: string) {
    super()

    this.message = `Schema registry error: code: ${errorCode} - ${message}`
    this.errorCode = errorCode
  }
}

/**
 * Configuration properties for the schema api client
 * TODO: allow/forward all configurations for http.request (not just the agent)
 */
export interface SchemaApiClientConfiguration {
  baseUrl: string
  username?: string
  password?: string
  agent?: httpRequest.Agent | httpsRequest.Agent
}

type RequestOptions = httpsRequest.RequestOptions | httpRequest.RequestOptions

/**
 * Full schema definition. Includes everything to know about a given schema version.
 */
export interface SchemaDefinition {
  subject: string
  id: number
  version: number
  /**
   * serialized schema representation
   */
  schema?: string
  /**
   * schema type (if not given schema registry will assume AVRO)
   */
  schemaType?: SchemaType
}

/**
 * schema registry client to interact with confluent schema registry
 * as specified here: https://docs.confluent.io/platform/current/schema-registry/develop/api.html
 *
 * Note: not all methods are implemented yet
 */
export class SchemaRegistryClient {
  baseRequestOptions: RequestOptions
  requester: typeof httpRequest | typeof httpsRequest
  basePath: string

  /**
   * create new SchemaRegistryClient instance
   */
  constructor(options: SchemaApiClientConfiguration) {
    const parsed = new URL(options.baseUrl)

    this.requester = parsed.protocol.startsWith("https") ? httpsRequest : httpRequest
    this.basePath = parsed.pathname !== null ? parsed.pathname : "/"

    const username = options.username ?? parsed.username
    const password = options.password ?? parsed.password

    this.baseRequestOptions = {
      host: parsed.hostname,
      port: parsed.port,
      headers: {
        Accept: "application/vnd.schemaregistry.v1+json",
        "Content-Type": "application/vnd.schemaregistry.v1+json",
      },
      agent: options.agent,
      auth: username && password ? `${username}:${password}` : null,
    }
  }

  // schemas section
  /**
   * Get a schema by its id
   * @param {number} schemaId id of the schema to fetch
   * @returns serialized schema information and schema type
   */
  async getSchemaById(schemaId: number): Promise<{ schema: string; schemaType: SchemaType }> {
    const path = `${this.basePath}schemas/ids/${schemaId}`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(requestOptions))
  }

  /**
   * Get types of possible schemas types
   * @returns array of possible schema types (typically: AVRO, PROTOBUF and JSON)
   */
  async getSchemaTypes(): Promise<Array<string>> {
    const path = `${this.basePath}schemas/types`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(requestOptions))
  }

  /**
   * Get subject/version pairs for given id
   * @param schemaId version of schema registered
   * @returns subject and version number for the schema identified by the id
   */
  async listVersionsForId(schemaId: number): Promise<Array<{ subject: string; version: number }>> {
    const path = `${this.basePath}schemas/ids/${schemaId}/versions`

    const versionListRequestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(versionListRequestOptions))
  }

  // subject section
  /**
   * Get list of available subjects
   * @returns array of registered subjects
   */
  async listSubjects(): Promise<Array<string>> {
    const path = `${this.basePath}subjects`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(requestOptions))
  }

  /**
   * Get list of versions registered under the specified subject
   * @param subject subject name
   * @returns array of schema versions
   */
  async listVersionsForSubject(subject: string): Promise<Array<number>> {
    const versionListRequestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path: `${this.basePath}subjects/${subject}/versions`,
    }

    return JSON.parse(await this.request(versionListRequestOptions))
  }

  /**
   * Deletes a schema.
   * Note: Should only be used in development mode. Don't delete schemas in production.
   * @param subject subject name
   * @returns list of deleted schema versions
   */
  async deleteSubject(subject: string, permanent: boolean = false): Promise<Array<number>> {
    const path = `${this.basePath}subjects/${subject}${permanent ? "?permanent=true" : ""}`

    const versionListRequestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method: "DELETE",
      path,
    }

    return JSON.parse(await this.request(versionListRequestOptions))
  }

  /**
   * Get schema for subject and version
   * @param subject subject name
   * @param version optional version to retrieve (if not provided the latest version will be fetched)
   * @returns schema and its metadata
   */
  async getSchemaForSubjectAndVersion(subject: string, version?: number): Promise<SchemaDefinition> {
    const path = `${this.basePath}subjects/${subject}/versions/${version ?? "latest"}`

    const schemaInfoRequestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return JSON.parse(await this.request(schemaInfoRequestOptions))
  }

  /**
   * Alias for getSchemaForSubjectAndVersion with version = latest
   * @param subject subject name
   * @returns schema and its metadata
   */
  async getLatestVersionForSubject(subject: string): ReturnType<SchemaRegistryClient["getSchemaForSubjectAndVersion"]> {
    return await this.getSchemaForSubjectAndVersion(subject)
  }

  /**
   * Get schema for subject and version
   * @param subject subject name
   * @param version schema version
   * @returns serialized schema
   */
  async getRawSchemaForSubjectAndVersion(subject: string, version: number): Promise<string> {
    const path = `${this.basePath}subjects/${subject}/versions/${version}/schema`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      path,
    }

    return await this.request(requestOptions)
  }

  /**
   * Register schema in registry
   *
   * Note: The specification says it will return subject, id, version and the schema itself.
   *       Factually it will only return the id of the newly created schema.
   * @param subject subject name
   * @param schema schema metadata and serialized schema representation
   * @returns schema metadata (or just the id of the newly created schema)
   */
  async registerSchema(
    subject: string,
    schema: { schema: string; schemaType: string; references?: any }
  ): Promise<SchemaDefinition> {
    const path = `${this.basePath}subjects/${subject}/versions`

    const body = JSON.stringify(schema)

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method: "POST",
      headers: { ...this.baseRequestOptions.headers },
      path,
    }

    return JSON.parse(await this.request(requestOptions, body))
  }

  /**
   * Check if a schema has already been registered under the specified subject.
   * If so, this returns the schema string along with its globally unique identifier, its version under this subject and the subject name.
   * @param subject subject name
   * @param schema serialized schema representation
   * @returns schema metadata and serialized schema
   */
  async checkSchema(
    subject: string,
    schema: { schema: string; schemaType?: string; references?: any }
  ): Promise<SchemaDefinition> {
    const path = `${this.basePath}subjects/${subject}`

    const body = JSON.stringify(schema)

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method: "POST",
      headers: { ...this.baseRequestOptions.headers },
      path,
    }

    return JSON.parse(await this.request(requestOptions, body))
  }

  // TODO: DELETE /subjects/(string: subject)/versions/(versionId: version)

  // TODO: GET /subjects/(string: subject)/versions/{versionId: version}/referencedby

  // TODO: mode section

  // compatibility section
  /**
   * Check a schema for compatibility
   * @param subject
   * @param version
   * @param schema
   * @param verbose
   * @returns whether or not the schema is compatible with the provided schema, given the compatibility mode for the subject
   */
  async testCompatibility(
    subject: string,
    version: number | "latest",
    schema: { schema: string; schemaType?: string; references?: any },
    verbose?: boolean
  ): Promise<boolean> {
    const path = `${this.basePath}compatibility/subjects/${subject}/versions/${version}${
      verbose ? "?verbose=true" : ""
    }`
    const method = "POST"
    const body = JSON.stringify(schema)

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method,
      headers: { ...this.baseRequestOptions.headers },
      path,
    }

    return JSON.parse(await this.request(requestOptions, body)).is_compatible
  }

  // config section
  /**
   * set schema registry default compatibility mode
   * @param compatibility new default compatibility mode
   * @returns new compatibility mode as echoed by the schema registry
   */
  async setConfig(compatibility: CompatibilityMode) {
    const path = `${this.basePath}config`
    const method = "PUT"
    const body = JSON.stringify({ compatibility })

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method,
      headers: { ...this.baseRequestOptions.headers },
      path,
    }

    return JSON.parse(await this.request(requestOptions, body)).compatibility
  }

  /**
   * get schema registry default compatibility mode
   */
  async getConfig(): Promise<CompatibilityMode> {
    const path = `${this.basePath}config`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      headers: { ...this.baseRequestOptions.headers },
      path,
    }

    return JSON.parse(await this.request(requestOptions)).compatibilityLevel
  }

  /**
   * set compatibility mode for a subject
   * @param subject name of the subject
   * @param compatibility new compatibility mode
   * @returns new compatibility mode as echoed by the schema registry
   */
  async setSubjectConfig(subject: string, compatibility: CompatibilityMode): Promise<CompatibilityMode> {
    const path = `${this.basePath}config/${subject}`
    const method = "PUT"
    const body = JSON.stringify({ compatibility })

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      method,
      headers: { ...this.baseRequestOptions.headers },
      path,
    }

    return JSON.parse(await this.request(requestOptions, body)).compatibility
  }

  /**
   * get compatibility mode for a subject
   * @param subject name of the subject
   * @returns current compatibility mode for the subject
   */
  async getSubjectConfig(subject: string): Promise<CompatibilityMode> {
    const path = `${this.basePath}config/${subject}`

    const requestOptions: RequestOptions = {
      ...this.baseRequestOptions,
      headers: { ...this.baseRequestOptions.headers },
      path,
    }

    return JSON.parse(await this.request(requestOptions)).compatibilityLevel
  }

  /**
   * Internal http request helper
   */
  private request(requestOptions: RequestOptions, requestBody?: string) {
    if (requestBody && requestBody.length > 0) {
      requestOptions.headers = { ...requestOptions.headers, "Content-Length": Buffer.byteLength(requestBody) }
    }

    return new Promise<string>((resolve, reject) => {
      const req = this.requester
        .request(requestOptions, (res) => {
          let data = ""
          res.on("data", (d) => {
            data += d
          })
          res.on("error", (e) => {
            // response error
            reject(e)
          })
          res.on("end", () => {
            if (res.statusCode === 200) {
              return resolve(data)
            }
            if (data.length > 0) {
              try {
                let { error_code, message } = JSON.parse(data)
                // squash different 404 errors
                if ([404, 40401, 40403].includes(error_code)) {
                  error_code = 404
                }
                return reject(new SchemaRegistryError(error_code, message))
              } catch (e) {
                return reject(e)
              }
            } else {
              return reject(new Error("Invalid schema registry response"))
            }
          })
        })
        .on("error", (e) => {
          // request error
          reject(e)
        })
      if (requestBody) {
        req.write(requestBody)
      }
      req.end()
    })
  }
}
