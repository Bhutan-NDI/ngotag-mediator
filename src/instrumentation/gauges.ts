import { LogLevel } from '@credo-ts/core'

import { emitStructured } from '../logger/StructuredLogger'
import { snapshotAndReset } from './metrics'

const GAUGE_INTERVAL_MS = 10_000

let _gaugeTimer: ReturnType<typeof setInterval> | null = null

export function startGauges(): void {
  if (_gaugeTimer) return
  _gaugeTimer = setInterval(() => {
    const snap = snapshotAndReset()
    emitStructured(LogLevel.info, {
      hop: 'mediator.gauge.snapshot',
      flow: 'lifecycle',
      ...snap,
    })
  }, GAUGE_INTERVAL_MS)
  // Don't block process exit
  if (_gaugeTimer.unref) _gaugeTimer.unref()
}

export function stopGauges(): void {
  if (_gaugeTimer) {
    clearInterval(_gaugeTimer)
    _gaugeTimer = null
  }
}
