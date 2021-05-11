/**
 * Prefix kafka message with the correct preamble for the given schema id
 * @param {Buffer} encodedMessage already encoded message
 * @param {number} schemaId
 * @returns {Buffer}
 */
export const kafkaEncode = (schemaId: number, encodedMessage: Buffer): Buffer => {
  if (!(encodedMessage instanceof Buffer)) {
    throw new Error("encoded message must be of type Buffer")
  }

  // Allocate buffer for encoded kafka event (1 byte preable + 4 byte messageid + sizeof(encodedMessage))
  const message = Buffer.alloc(encodedMessage.length + 5)

  message.writeUInt8(0)
  message.writeUInt32BE(schemaId, 1)
  encodedMessage.copy(message, 5)

  return message
}

/**
 * Decode the schema preamble and return schemaId anf payload
 * @param {Buffer} rawMessage Unencoded message from kafka event
 * @returns schemaId and unencoded payload
 */
export const kafkaDecode = (rawMessage: Buffer): { schemaId?: number; payload: Buffer } => {
  if (rawMessage.readUInt8(0) !== 0) {
    // throw new Error(`Missing schema preamble.`)
    return { payload: rawMessage }
  }

  const schemaId = rawMessage.readUInt32BE(1)
  const payload = rawMessage.slice(5)

  return { schemaId, payload }
}
