import { AddressInfo, createServer } from "net"

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
