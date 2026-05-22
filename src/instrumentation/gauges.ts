import { LogLevel } from '@credo-ts/core'

import { emitStructured } from '../logger/StructuredLogger'
import { snapshotAndReset, getQueueAccessor } from './metrics'

const GAUGE_INTERVAL_MS = 10_000

let _gaugeTimer: ReturnType<typeof setInterval> | null = null

export function startGauges(): void {
  if (_gaugeTimer) return
  _gaugeTimer = setInterval(async () => {
    const snap = snapshotAndReset()

    // Queue stats: fetch with a 1s timeout so a slow DB never stalls the gauge tick.
    let queueFields: Record<string, unknown> = {}
    const accessor = getQueueAccessor()
    if (accessor) {
      try {
        const qSnap = await Promise.race([
          accessor(),
          new Promise<null>((resolve) => setTimeout(() => resolve(null), 1000)),
        ])
        if (qSnap) {
          queueFields = {
            queue_depth_total: qSnap.total,
            queue_oldest_age_ms: qSnap.oldestAgeMs,
            queue_depth_top10: qSnap.top10,
          }
        }
      } catch {
        // ignore — snapshot is best-effort
      }
    }

    emitStructured(LogLevel.info, {
      hop: 'mediator.gauge.snapshot',
      flow: 'lifecycle',
      ...snap,
      ...queueFields,
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
