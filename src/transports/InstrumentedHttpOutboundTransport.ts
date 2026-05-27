import { HttpOutboundTransport, LogLevel } from '@credo-ts/core'
import type { OutboundPackage } from '@credo-ts/core'

import { emitStructured, makeSpanId, monoNow, durationMs, tryExtractJweFp } from '../logger/StructuredLogger'
import { requestContext } from '../instrumentation/requestContext'
import { recordOutboundMs } from '../instrumentation/metrics'

export class InstrumentedHttpOutboundTransport extends HttpOutboundTransport {
  async sendMessage(outboundPackage: OutboundPackage): Promise<void> {
    const spanId = makeSpanId()
    const startMono = monoNow()
    const targetUrl = outboundPackage.endpoint ?? ''
    const jweFp = tryExtractJweFp(outboundPackage.payload)
    const jweFpIn = requestContext.getStore()?.jweFpIn ?? ''

    emitStructured(LogLevel.trace, {
      hop: 'mediator.outbound.send.start',
      span_id: spanId,
      jwe_fp: jweFp,
      jwe_fp_in: jweFpIn,
      conn_id: outboundPackage.connectionId ?? '',
      target_url: targetUrl,
    })

    try {
      await super.sendMessage(outboundPackage)
      const elapsed = durationMs(startMono)
      recordOutboundMs(elapsed)
      emitStructured(LogLevel.trace, {
        hop: 'mediator.outbound.send.end',
        span_id: spanId,
        jwe_fp: jweFp,
        jwe_fp_in: jweFpIn,
        conn_id: outboundPackage.connectionId ?? '',
        target_url: targetUrl,
        duration_ms: elapsed,
        status: 'ok',
      })
    } catch (err) {
      emitStructured(LogLevel.trace, {
        hop: 'mediator.outbound.send.end',
        span_id: spanId,
        jwe_fp: jweFp,
        jwe_fp_in: jweFpIn,
        conn_id: outboundPackage.connectionId ?? '',
        target_url: targetUrl,
        duration_ms: durationMs(startMono),
        status: 'error',
        notes: err instanceof Error ? err.message.slice(0, 120) : 'unknown error',
      })
      throw err
    }
  }
}
