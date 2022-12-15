import { parse, Type } from "avsc"
import { StartedDockerComposeEnvironment } from "testcontainers"

import { KafkaRegistryHelper } from "../kafka-registry-helper"
import { SchemaType } from "../schema-registry-client"
import { up } from "./helper"
import { Consumer, Kafka, KafkaMessage, Producer } from "kafkajs"

let testcontainers: StartedDockerComposeEnvironment
let schemaRegistryPort: number
let brokerPort: number

beforeAll(async () => {
  const env = await up()
  testcontainers = env.testcontainers
  schemaRegistryPort = env.schemaRegistryPort
  brokerPort = env.brokerPort
  //   schemaRegistryPort = 56785
  //   brokerPort = 9092
}, 15000 /* increase timeout to 10 minutes (docker compose from scratch will probably take longer) */)

afterAll(async () => {
  await testcontainers?.down()
}, 60000)

describe("kafkajs producer/consumer test (with AVRO)", () => {
  let registry: KafkaRegistryHelper
  let kafka: Kafka
  let producer: Producer
  let consumer: Consumer

  const topic = "schematic-kafka-test-topic"
  const key = { id: 0x0815 }
  const value = { text: "Text message" }

  // schema preparation (registry requires the schema to have a name)
  const keySchemaJson = Type.forValue(key).toJSON()
  keySchemaJson["name"] = `key`
  const keySchema = JSON.stringify(keySchemaJson)
  const valueSchemaJson = Type.forValue(value).toJSON()
  valueSchemaJson["name"] = `value`
  const valueSchema = JSON.stringify(valueSchemaJson)

  beforeAll(async () => {
    registry = new KafkaRegistryHelper({ baseUrl: `http://localhost:${schemaRegistryPort}` }).withSchemaHandler(
      SchemaType.AVRO,
      (schema: string) => {
        const avsc: Type = parse(schema) // could add all kinds of configurations here
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

    // handle connect / subscribe of producers and subscribers here to keep the actual test simple
    kafka = new Kafka({
      clientId: "my-app",
      brokers: [`localhost:${brokerPort}`],
    })

    producer = kafka.producer()
    await producer.connect()

    consumer = kafka.consumer({ groupId: "schematic-kafka-test-group" })
    await consumer.connect()

    await consumer.subscribe({ topic })
  })

  afterAll(async () => {
    await producer?.disconnect()

    await consumer?.stop()
    await consumer?.disconnect()
  }, 30000)

  it("sends and receives encoded message via kafka", async () => {
    // encode key/value
    const encodedKey = await registry.encodeForSubject(`${topic}-key`, key, SchemaType.AVRO, keySchema)
    const encodedValue = await registry.encodeForSubject(`${topic}-value`, value, SchemaType.AVRO, valueSchema)

    // event receiver
    const messageReceived = new Promise<KafkaMessage>(async (resolve) => {
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          resolve(message)
        },
      })
    })

    // event sender (delay by 2 seconds so that kafka has a chance to elect a topic leader)
    const messageProduced = new Promise((resolve) => setTimeout(resolve, 2000)).then(() =>
      producer.send({
        topic,
        messages: [{ value: encodedValue, key: encodedKey }],
      })
    )

    // resolve sender / receiver
    const awaited = await Promise.all<unknown>([messageReceived, messageProduced])
    const [message] = awaited as [KafkaMessage]

    // decode key/value
    const decodedKey = await registry.decode(message.key!)
    const decodedValue = await registry.decode(message.value!)

    expect(decodedKey).toEqual(key)
    expect(decodedValue).toEqual(value)
  })
})
