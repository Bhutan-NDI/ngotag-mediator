import { LogLevel } from '@credo-ts/core'
import * as dotenv from 'dotenv'
dotenv.config()

export const AGENT_PORT = process.env.AGENT_PORT ? Number(process.env.AGENT_PORT) : 3000
export const AGENT_NAME = process.env.AGENT_NAME || 'CREDEBL Mediator'
export const WALLET_NAME = process.env.WALLET_NAME || 'credebl-mediator-dev'
export const WALLET_KEY = process.env.WALLET_KEY || 'credebl-mediator-dev'
export const AGENT_ENDPOINTS = process.env.AGENT_ENDPOINTS?.split(',') ?? [
  `http://localhost:${AGENT_PORT}`,
  `ws://localhost:${AGENT_PORT}`,
]

export const POSTGRES_HOST = process.env.POSTGRES_HOST
export const POSTGRES_USER = process.env.POSTGRES_USER
export const POSTGRES_PASSWORD = process.env.POSTGRES_PASSWORD
export const POSTGRES_ADMIN_USER = process.env.POSTGRES_ADMIN_USER
export const POSTGRES_ADMIN_PASSWORD = process.env.POSTGRES_ADMIN_PASSWORD

export const INVITATION_URL = process.env.INVITATION_URL

export const LOG_LEVEL = process.env.LOG_LEVEL
? (LogLevel[process.env.LOG_LEVEL as keyof typeof LogLevel] ?? LogLevel.debug)
: LogLevel.debug 

export const IS_DEV = process.env.NODE_ENV === 'development'

export const USE_PUSH_NOTIFICATIONS = process.env.USE_PUSH_NOTIFICATIONS === 'true'

export const NOTIFICATION_WEBHOOK_URL = process.env.NOTIFICATION_WEBHOOK_URL || 'http://localhost:5000'

export const MESSAGE_FORWARDING_STRATEGY = process.env.MESSAGE_FORWARDING_STRATEGY

export const WALLET_DB_MAX_CONNECTIONS = Number(process.env.WALLET_DB_MAX_CONNECTIONS) || 10
export const WALLET_DB_MIN_CONNECTIONS = Number(process.env.WALLET_DB_MIN_CONNECTIONS) || 0
export const WALLET_DB_IDLE_TIMEOUT = Number(process.env.WALLET_DB_IDLE_TIMEOUT) || 0
export const WALLET_DB_CONNECT_TIMEOUT = Number(process.env.WALLET_DB_CONNECT_TIMEOUT) || 10

// Debug instrumentation admin endpoint.  Set this to a long random string in production.
// If unset, the /admin/log-level endpoint is disabled (returns 503).
export const ADMIN_TOKEN = process.env.ADMIN_TOKEN

// WebSocket keepalive (see socketServer connection handler in src/agent.ts).
// Credo's WsInboundTransport sends no application-level ping, so idle mobile sockets are
// silently dropped by NAT / the ALB idle timeout (~60s). Every drop evicts the LiveMode
// session (MessagePickupSessionService removes it on TransportSessionRemoved), which is why
// QueueAndLiveModeDelivery falls back to slow pickup. A ping/pong heartbeat keeps healthy
// sockets alive so the live session persists between forwards.
export const WS_KEEPALIVE_ENABLED = process.env.WS_KEEPALIVE_ENABLED !== 'false'
export const WS_KEEPALIVE_INTERVAL_MS = (() => {
  const raw = Number(process.env.WS_KEEPALIVE_INTERVAL_MS)
  // Guard against negative, zero, NaN, or Infinity — any of which would cause setInterval
  // to run immediately and continuously, terminating healthy sockets. Minimum is 5 s.
  return Number.isFinite(raw) && raw > 0 ? Math.max(raw, 5_000) : 30_000
})()
