import { kafkaDecode, kafkaEncode } from "./kafka-helper"

it("encodes/decodes", () => {
  const result = kafkaEncode(1, Buffer.from("Hello World!"))
  const { schemaId, payload } = kafkaDecode(result)
  expect(schemaId).toEqual(1)
  expect(payload.toString()).toEqual("Hello World!")
})

it("can handle payloads that don't use the schema-notation", () => {
  const { schemaId, payload } = kafkaDecode(Buffer.from("Hello World!"))
  expect(schemaId).toBeUndefined()
  expect(payload.toString()).toEqual("Hello World!")
})

it("should not allow an argument that is not a buffer", () => {
  // @ts-ignore
  expect(() => kafkaEncode(1, "not a buffer")).toThrow()
})
