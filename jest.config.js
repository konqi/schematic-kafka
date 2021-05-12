module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  globals: {
    "ts-jest": {
      tsconfig: "tsconfig.spec.json",
    },
  },
  testPathIgnorePatterns: ["/node_modules/", "src/testcontainers"],
}
