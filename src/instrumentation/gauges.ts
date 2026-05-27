import { LogLevel } from '@credo-ts/core'

import { emitStructured } from '../logger/StructuredLogger'
import { snapshotAndReset, getQueueAccessor, getWalletPoolAccessor } from './metrics'

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

    // Wallet pool stats: populated if registerWalletPoolAccessor has been called.
    let walletPoolFields: Record<string, unknown> = {}
    const walletPoolAccessor = getWalletPoolAccessor()
    if (walletPoolAccessor) {
      try {
        const poolSnap = await Promise.race([
          walletPoolAccessor(),
          new Promise<null>((resolve) => setTimeout(() => resolve(null), 500)),
        ])
        if (poolSnap) {
          walletPoolFields = {
            wallet_pool_in_use: poolSnap.in_use,
            wallet_pool_idle: poolSnap.idle,
            wallet_pool_waiting: poolSnap.waiting,
          }
        }
      } catch {
        // ignore — snapshot is best-effort
      }
    }

    emitStructured(LogLevel.trace, {
      hop: 'mediator.gauge.snapshot',
      flow: 'lifecycle',
      ...snap,
      ...queueFields,
      ...walletPoolFields,
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
