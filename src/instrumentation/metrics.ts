// Shared in-memory counters and rolling windows used by both the gauge emitter
// and the instrumented transports / storage modules.

let _wsSessionsActive = 0
const _outboundMs: number[] = []
let _queueWritesLast10s = 0
let _wsOpened10s = 0
let _wsClosed10s = 0

export function wsSessionOpened(): void {
  _wsSessionsActive++
  _wsOpened10s++
}

export function wsSessionClosed(): void {
  _wsSessionsActive = Math.max(0, _wsSessionsActive - 1)
  _wsClosed10s++
}

export function recordOutboundMs(ms: number): void {
  _outboundMs.push(ms)
  if (_outboundMs.length > 1000) _outboundMs.shift()
}

export function recordQueueWrite(): void {
  _queueWritesLast10s++
}

export interface GaugeSnapshot {
  ws_sessions_active: number
  ws_sessions_opened_10s: number
  ws_sessions_closed_10s: number
  outbound_p50_ms: number | null
  outbound_p95_ms: number | null
  outbound_sample_n: number
  queue_writes_10s: number
}

export function snapshotAndReset(): GaugeSnapshot {
  const sorted = [..._outboundMs].sort((a, b) => a - b)
  const n = sorted.length
  const p50 = n > 0 ? sorted[Math.floor(n * 0.5)] : null
  const p95 = n > 0 ? sorted[Math.floor(n * 0.95)] : null

  const snap: GaugeSnapshot = {
    ws_sessions_active: _wsSessionsActive,
    ws_sessions_opened_10s: _wsOpened10s,
    ws_sessions_closed_10s: _wsClosed10s,
    outbound_p50_ms: p50,
    outbound_p95_ms: p95,
    outbound_sample_n: n,
    queue_writes_10s: _queueWritesLast10s,
  }

  // Reset rolling counters
  _outboundMs.length = 0
  _wsOpened10s = 0
  _wsClosed10s = 0
  _queueWritesLast10s = 0

  return snap
}
