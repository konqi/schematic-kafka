module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  globals: {
    "ts-jest": {
      tsconfig: "tsconfig.spec.json",
    },
  },
  testRegex: "testcontainers/.*(\\.|\\/)(test|spec)\\.[jt]sx?$",
}
