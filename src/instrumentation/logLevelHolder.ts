import { LogLevel } from '@credo-ts/core'

// Default threshold for emitStructured() instrumentation hops. At `warn`, all the info/debug
// trace hops (queue.write, pickup.batch.dispatch, push.send, gauge.snapshot, livemode/transport
// session events, etc.) are suppressed — production stays quiet by default. Raise it to `info`
// or `debug` at runtime via POST /admin/log-level when investigating; no redeploy needed.
let _level: LogLevel = LogLevel.warn

export function getDebugLogLevel(): LogLevel {
  return _level
}

export function setDebugLogLevel(level: LogLevel): void {
  _level = level
}
