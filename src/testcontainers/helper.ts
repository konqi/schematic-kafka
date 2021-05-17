import { AddressInfo, createServer } from "net"
import { DockerComposeEnvironment, Wait } from "testcontainers"
import { Type } from "protobufjs"

const TAG = "5.5.4"

export const findPort = () =>
  new Promise((resolve) => {
    const server = createServer()
    server.listen(0, () => {
      const { port } = server.address() as AddressInfo
      server.close(() => {
        resolve(port)
      })
    })
  })

export const up = async () => {
  const kafkaPort = await findPort()

  const testcontainers = await new DockerComposeEnvironment(".", "docker-compose.yml")
    .withEnv("TAG", TAG)
    .withEnv("KAFKA_PORT", `${kafkaPort}`)
    .withWaitStrategy("zookeeper_1", Wait.forLogMessage("binding to port"))
    .withWaitStrategy("broker_1", Wait.forLogMessage("Awaiting socket connections"))
    .withStartupTimeout(1000 * 60 * 3)
    .up()

  const schemaRegistryPort = testcontainers.getContainer("schema-registry_1").getMappedPort(8081)
  const brokerPort = testcontainers.getContainer("broker_1").getMappedPort(9092)

  return {
    testcontainers,
    schemaRegistryPort,
    brokerPort,
  }
}

// function to traverse protobuf definition to find all the types (there should only be one)
export const findTypes = (src: any) => {
  const types: string[] = []

  const dive = (src: any) => {
    if (src instanceof Type) {
      types.push(src.name)
    } else if (src.nested) {
      dive(src.nested)
    } else if (typeof src === "object") {
      Object.values(src).map(dive)
    }
  }

  dive(src)

  return types
}
