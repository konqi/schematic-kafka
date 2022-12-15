import { AddressInfo, createServer } from "net"
import { DockerComposeEnvironment, Wait } from "testcontainers"
import { Type } from "protobufjs"

const TAG = "7.3.0"

export const findPort = () =>
  new Promise<number>((resolve) => {
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
    .withEnvironment({ TAG: TAG, KAFKA_PORT: `${kafkaPort}` })
    .withWaitStrategy("zookeeper", Wait.forLogMessage("binding to port"))
    .withWaitStrategy("broker", Wait.forLogMessage("Ready to serve as the new controller"))
    .withStartupTimeout(1000 * 60 * 3)
    .up()

  const schemaRegistryPort = testcontainers.getContainer("schema-registry").getMappedPort(8081)

  return {
    testcontainers,
    schemaRegistryPort,
    brokerPort: kafkaPort,
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
