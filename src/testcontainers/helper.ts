import { AddressInfo, createServer } from "net"
import { DockerComposeEnvironment, Wait } from "testcontainers"

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

  return {
    testcontainers,
    schemaRegistryPort,
  }
}
