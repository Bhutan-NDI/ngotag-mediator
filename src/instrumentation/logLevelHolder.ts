import { LogLevel } from '@credo-ts/core'

// Default threshold for emitStructured() instrumentation hops. All per-message hops
// (queue.write, pickup.batch.dispatch, push.send, gauge.snapshot, livemode/transport session
// events, etc.) emit at `trace` and remain suppressed at this `info` default, keeping
// production quiet. The startup mediator.config.dump event emits at `info` and therefore
// always surfaces at boot. To capture trace hops, raise the level at runtime via
// POST /admin/log-level; no redeploy needed.
let _level: LogLevel = LogLevel.info

export function getDebugLogLevel(): LogLevel {
  return _level
}

export function setDebugLogLevel(level: LogLevel): void {
  _level = level
}
