import { parse, Type as AVSCInstance } from "avsc"
import { StartedDockerComposeEnvironment } from "testcontainers"

import { KafkaRegistryHelper } from "../kafka-registry-helper"
import { SchemaType } from "../schema-registry-client"
import { up } from "./helper"
import { Consumer, Kafka, KafkaMessage, Producer } from "kafkajs"

let testcontainers: StartedDockerComposeEnvironment
let schemaRegistryPort: number
let brokerPort: number

beforeAll(async () => {
  // increase timeout to 10 minutes (docker compose from scratch will probably take longer)
  jest.setTimeout(1000 * 60 * 10)

  const env = await up()
  testcontainers = env.testcontainers
  schemaRegistryPort = env.schemaRegistryPort
  brokerPort = env.brokerPort

  jest.setTimeout(15000)
})

afterAll(async () => {
  jest.setTimeout(60000)

  await testcontainers?.down()
})

describe("kafkajs producer/consumer test (with AVRO)", () => {
  let registry: KafkaRegistryHelper

  const topic = "schematic-kafka-test-topic"
  const key = { id: 0x0815 }
  const value = { text: "Text message" }

  const kafka = new Kafka({
    clientId: "my-app",
    brokers: [`localhost:${brokerPort}`],
  })

  let producer: Producer
  let consumer: Consumer

  beforeAll(async () => {
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

  afterAll(async () => {
    await producer?.disconnect()

    await consumer?.stop()
    await consumer?.disconnect()
  })

  it.concurrent("producer", async () => {
    producer = kafka.producer()
    await producer.connect()

    // add names to schema
    const keySchema = AVSCInstance.forValue(key).toJSON()
    keySchema["name"] = `key`
    const valueSchema = AVSCInstance.forValue(value).toJSON()
    valueSchema["name"] = `value`

    console.log(JSON.stringify(keySchema))
    console.log(JSON.stringify(valueSchema))

    const encodedKey = await registry.encodeForSubject(`${topic}-key`, key, SchemaType.AVRO, JSON.stringify(keySchema))
    const encodedValue = await registry.encodeForSubject(
      `${topic}-value`,
      value,
      SchemaType.AVRO,
      JSON.stringify(valueSchema)
    )

    await producer.send({
      topic,
      messages: [{ value: encodedValue, key: encodedKey }],
    })
  })

  it.concurrent("consumer", async () => {
    consumer = kafka.consumer({ groupId: "schematic-kafka-test-group" })
    await consumer.connect()

    await consumer.subscribe({ topic, fromBeginning: true })

    const message = await new Promise<KafkaMessage>(async (resolve) => {
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          resolve(message)
        },
      })
    })

    const decodedKey = await registry.decode(message.key)
    const decodedValue = await registry.decode(message.value)

    expect(decodedKey).toEqual(key)
    expect(decodedValue).toEqual(value)
  })
})
