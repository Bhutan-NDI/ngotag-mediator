import { LogLevel } from '@credo-ts/core'

// Default threshold for emitStructured() instrumentation hops. The instrumentation emits at
// `trace`, so at the `warn` default every hop (queue.write, pickup.batch.dispatch, push.send,
// gauge.snapshot, livemode/transport session events, etc.) is suppressed — production stays
// quiet by default. To capture them, set the level to `trace` at runtime via
// POST /admin/log-level (the deepest, explicitly-enabled level); no redeploy needed.
let _level: LogLevel = LogLevel.warn

export function getDebugLogLevel(): LogLevel {
  return _level
}

export function setDebugLogLevel(level: LogLevel): void {
  _level = level
}
