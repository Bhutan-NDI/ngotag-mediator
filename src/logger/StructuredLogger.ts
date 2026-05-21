import { LogLevel } from '@credo-ts/core'
import * as os from 'os'

import { getDebugLogLevel } from '../instrumentation/logLevelHolder'

export type HopName =
  | 'mediator.config.dump'
  | 'mediator.http.inbound.received'
  | 'mediator.ws.inbound.received'
  | 'mediator.ws.session.opened'
  | 'mediator.ws.session.closed'
  | 'mediator.forward.strategy.decision'
  | 'mediator.queue.write.start'
  | 'mediator.queue.write.end'
  | 'mediator.live.delivery.start'
  | 'mediator.live.delivery.end'
  | 'mediator.outbound.send.start'
  | 'mediator.outbound.send.end'
  | 'mediator.pickup.request.received'
  | 'mediator.pickup.batch.dispatch.start'
  | 'mediator.pickup.batch.dispatch.end'
  | 'mediator.push.send.start'
  | 'mediator.push.send.end'
  | 'mediator.gauge.snapshot'

export type FlowType =
  | 'connection'
  | 'issuance'
  | 'verification'
  | 'pickup'
  | 'mediation-coord'
  | 'lifecycle'

export interface StructuredLogLine {
  hop: HopName
  flow?: FlowType
  thread_id?: string
  outer_msg_id?: string
  recipient_key_short?: string
  conn_id?: string
  span_id?: string
  duration_ms?: number
  notes?: string
  [key: string]: unknown
}

const TASK_HOST = process.env.ECS_CONTAINER_METADATA_URI ? process.env.HOSTNAME || os.hostname() : os.hostname()

export function emitStructured(level: LogLevel, line: StructuredLogLine): void {
  if (level < getDebugLogLevel()) return

  const out: Record<string, unknown> = {
    ts: new Date().toISOString(),
    ts_mono_ns: Number(process.hrtime.bigint()),
    service: 'mediator',
    task_host: TASK_HOST,
    ...line,
  }

  // Remove undefined fields to keep lines compact
  for (const key of Object.keys(out)) {
    if (out[key] === undefined || out[key] === '') {
      if (key !== 'thread_id' && key !== 'outer_msg_id') delete out[key]
    }
  }

  process.stdout.write(JSON.stringify(out) + '\n')
}

export function makeSpanId(): string {
  return Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 10)
}

export function monoNow(): number {
  return Number(process.hrtime.bigint())
}

export function durationMs(startMono: number): number {
  return Math.round((Number(process.hrtime.bigint()) - startMono) / 1e6)
}

export function truncateKey(key: string): string {
  if (key.length <= 14) return key
  return key.slice(0, 6) + '…' + key.slice(-6)
}

export function tryExtractRecipientKeyShort(rawBody: string): string {
  try {
    const parsed = JSON.parse(rawBody) as Record<string, unknown>
    // recipients is a top-level JWE JSON Serialization field, not inside the protected header
    const recipients = parsed['recipients']
    if (!Array.isArray(recipients) || recipients.length === 0) return ''
    const first = recipients[0] as Record<string, unknown>
    const hdr = first['header'] as Record<string, unknown> | undefined
    if (!hdr) return ''
    const kid = hdr['kid']
    if (typeof kid !== 'string') return ''
    return truncateKey(kid)
  } catch {
    return ''
  }
}
