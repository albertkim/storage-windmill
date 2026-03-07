import { execSync } from "child_process"
import { existsSync } from "fs"

// Requires ansible-vault and a .vault_pass file in the project root.
// Encrypts .env to .env.encrypted (for safe commiting).

const envFile = ".env"
const encryptedEnvFile = ".env.encrypted"

if (!existsSync(envFile)) {
  console.log(`Skipping: ${envFile} does not exist`)
  process.exit(0)
}

console.log(`Encrypting ${envFile} to ${encryptedEnvFile}`)
execSync(`ansible-vault encrypt --vault-password-file .vault_pass --output ${encryptedEnvFile} ${envFile}`)
