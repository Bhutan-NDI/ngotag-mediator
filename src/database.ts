import type { AskarWalletPostgresStorageConfig } from '@credo-ts/askar/build/wallet'

import { POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST,WALLET_DB_MAX_CONNECTIONS, WALLET_DB_MIN_CONNECTIONS, WALLET_DB_IDLE_TIMEOUT, WALLET_DB_CONNECT_TIMEOUT } from './constants'

export const askarPostgresConfig: AskarWalletPostgresStorageConfig = {
  // AskarWalletPostgresStorageConfig defines interface for the Postgres plugin configuration.
  type: 'postgres',
  config: {
    host: POSTGRES_HOST as string,
    maxConnections: WALLET_DB_MAX_CONNECTIONS,
    minConnections: WALLET_DB_MIN_CONNECTIONS,
    idleTimeout: WALLET_DB_IDLE_TIMEOUT,
    connectTimeout: WALLET_DB_CONNECT_TIMEOUT,
  },
  credentials: {
    account: POSTGRES_USER as string,
    password: POSTGRES_PASSWORD as string,
    adminAccount: POSTGRES_USER as string,
    adminPassword: POSTGRES_PASSWORD as string,
  },
}
