import { kafkaDecode, kafkaEncode } from "./kafka-helper"

it("encodes/decodes", () => {
  // TODO
  const result = kafkaEncode(1, Buffer.from("Hello World!"))
  const { schemaId, payload } = kafkaDecode(result)
  expect(schemaId).toEqual(1)
  expect(payload.toString()).toEqual("Hello World!")
})

it("can handle no schema decodes", () => {
  const { schemaId, payload } = kafkaDecode(Buffer.from("Hello World!"))
  expect(schemaId).toBeUndefined()
  expect(payload.toString()).toEqual("Hello World!")
})
