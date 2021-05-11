import { kafkaEncode, kafkaDecode } from "./kafka-helper"
import { SchemaRegistryClient, SchemaRegistryError, SchemaType } from "./schema-registry-client"
import { FunctionCacher } from "./function-cacher"

/**
 * Helper class to cache calls to the SchemaRegistryClient
 */
class CachedSchemaRegistryClient extends SchemaRegistryClient {
  cacher = new FunctionCacher()

  constructor(...schemaRegistryClientOptions: ConstructorParameters<typeof SchemaRegistryClient>) {
    super(...schemaRegistryClientOptions)
  }

  checkSchema = this.cacher.createCachedFunction(super.checkSchema, [true, true], this)
  getLatestVersionForSubject = this.cacher.createCachedFunction(super.getLatestVersionForSubject, [true], this)
  getSchemaById = this.cacher.createCachedFunction(super.getSchemaById, [true], this)
}

/**
 * This is a "convenient" method to provide different schema
 * handlers (i.e. AVRO, JSON, PROTOBUF) into the ??
 */
type SchemaHandlerFactory = (
  schema: string
) => {
  decode(message: Buffer): any
  encode(message: any): Buffer
}

export class KafkaRegistryHelper {
  schemaHandlers = new Map<SchemaType, SchemaHandlerFactory>()
  readonly schemaRegistryClient: CachedSchemaRegistryClient

  constructor(...schemaRegistryOptions: ConstructorParameters<typeof SchemaRegistryClient>) {
    this.schemaRegistryClient = new CachedSchemaRegistryClient(...schemaRegistryOptions)
  }

  /**
   * Helper function to add a schema Handler in a syntactic sugary way
   * Note: The registry helper will call the factory every time a message is encoded/decoded
   *       It is your responsibility to handle caching of the schema handlers
   * @param schemaType
   * @param schemaHandlerFactory
   * @returns the KafkaRegistryHelper instance
   */
  withSchemaHandler(schemaType: SchemaType, schemaHandlerFactory: SchemaHandlerFactory) {
    this.schemaHandlers.set(schemaType, schemaHandlerFactory)
    return this
  }

  /**
   * Checks whether a schema is available in the schema registry, if not tries to register it
   * @param subject
   * @param schemaType
   * @param schema the registry needs the schema to be a string
   * @param references
   * @returns schema string and schemaId
   */
  private async makeSureSchemaIsRegistered(
    subject: string,
    schemaType: SchemaType = SchemaType.AVRO,
    schema?: string,
    references?: any
  ): Promise<{ id: number; schema: string; schemaType: SchemaType }> {
    // register schema or fetch schema
    let registrySchemaId = undefined
    let registrySchema = undefined
    let registrySchemaType = undefined
    if (schema) {
      try {
        const checkSchemaResult = await this.schemaRegistryClient.checkSchema(subject, {
          schemaType,
          schema,
          references,
        })
        registrySchemaId = checkSchemaResult.id
        registrySchema = checkSchemaResult.schema ?? schema
        registrySchemaType = checkSchemaResult.schemaType
      } catch (e) {
        if (e instanceof SchemaRegistryError && e.errorCode === 404) {
          // schema does not exist, need to create it
          const registerSchemaResult = await this.schemaRegistryClient.registerSchema(subject, {
            schemaType,
            schema,
            references,
          })
          registrySchemaId = registerSchemaResult.id
          // According to https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions
          // the register post should return schema, schemaType, references and id. However, the response only contains the id.
          registrySchema = registerSchemaResult.schema ?? schema
          registrySchemaType = registerSchemaResult.schemaType ?? schemaType
        } else {
          throw e
        }
      }
    } else {
      const { id, schema, schemaType } = await this.schemaRegistryClient.getLatestVersionForSubject(subject)
      registrySchemaId = id
      registrySchema = schema
      registrySchemaType = schemaType
    }

    return { id: registrySchemaId, schema: registrySchema, schemaType: registrySchemaType ?? schemaType }
  }

  /**
   * Encode a message with a given schema id
   * @param schemaId id of the schema
   * @param message formatted in whatever type SchemaHandler works with
   * @param schemaType (optional, default = AVRO) schema type can be given, mostly for safety reason
   * @returns message encoded with schema associated with schema id
   */
  async encodeForId(schemaId: number, message: any, schemaType: SchemaType = SchemaType.AVRO) {
    if (!this.schemaHandlers.get(schemaType)) {
      throw new Error(`No protocol handler for protocol ${schemaType}`)
    }

    const {
      schema: registrySchema,
      schemaType: registrySchemaType = SchemaType.AVRO,
    } = await this.schemaRegistryClient.getSchemaById(schemaId)

    if (schemaType !== registrySchemaType) {
      throw new Error("Mismatch between schemaType argument and schema registry schemaType")
    }

    return this.encodeMessage(schemaId, message, registrySchemaType, registrySchema)
  }

  /**
   * Encode a message by subject name
   * @param subject subject name (for kafka messages, don't forget the -key/-value postfix)
   * @param message formatted in whatever type SchemaHandler works with
   * @param schemaType (optional, default = AVRO) type of schema (e.g. PROTOBUG)
   * @param schema serialized representation of the message schema (this will be registered with registry if it doesn't exist there yet)
   * @param references additional schema references
   * @returns message encoded for given subject / schema (schema id will be determined automatically)
   */
  async encodeForSubject(
    subject: string,
    message: any,
    schemaType: SchemaType = SchemaType.AVRO,
    schema?: string,
    references?: any
  ): Promise<Buffer> {
    if (!this.schemaHandlers.get(schemaType)) {
      throw new Error(`No protocol handler for protocol ${schemaType}`)
    }

    const {
      id,
      schema: registrySchema,
      schemaType: registrySchemaType = SchemaType.AVRO,
    } = await this.makeSureSchemaIsRegistered(subject, schemaType, schema, references)

    if (schemaType !== registrySchemaType) {
      throw new Error("Mismatch between schemaType argument and schema registry schemaType")
    }

    return this.encodeMessage(id, message, registrySchemaType, registrySchema)
  }

  /**
   * decode a kafka event message
   * @param message message with or without schema preamble. Note: If the preamble is missing the payload will be passed through
   * @returns message in whatever format the SchemaHandler for the schemaType produces
   */
  async decode(message: Buffer): Promise<any> {
    const { schemaId, payload } = kafkaDecode(message)
    if (schemaId) {
      const { schema, schemaType } = await this.schemaRegistryClient.getSchemaById(schemaId)
      // if schemaType isn't provided, use avro (it's the default)
      return this.schemaHandlers
        .get(schemaType ?? SchemaType.AVRO)(schema)
        .decode(payload)
    } else {
      return payload
    }
  }

  /**
   * handle message encoding with schemaHandler
   * @param schemaId id of the schema
   * @param message formatted in whatever type SchemaHandler works with
   * @param schemaType schema type, schema handler for this type has to be registered
   * @param schema the schema as provided by the protocol handler
   * @returns message encoded with handler corresponding to the schemaType
   */
  private encodeMessage(schemaId: number, message: any, schemaType: SchemaType, schema: string) {
    const schemaHandler = this.schemaHandlers.get(schemaType)(schema)

    const encodedMessage = schemaHandler.encode(message)

    return kafkaEncode(schemaId, encodedMessage)
  }
}
