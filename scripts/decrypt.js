import { execSync } from "child_process"
import { existsSync } from "fs"

// Requires ansible-vault and a .vault_pass file in the project root.
// Decrypts .env.encrypted to .env.

const encryptedEnvFile = ".env.encrypted"
const envFile = ".env"

if (!existsSync(encryptedEnvFile)) {
  console.log(`Skipping: ${encryptedEnvFile} does not exist`)
  process.exit(0)
}

console.log(`Decrypting ${encryptedEnvFile} to ${envFile}`)
execSync(`ansible-vault decrypt --vault-password-file .vault_pass --output ${envFile} ${encryptedEnvFile}`)
