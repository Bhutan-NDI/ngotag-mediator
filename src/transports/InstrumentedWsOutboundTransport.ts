import { WsOutboundTransport, LogLevel } from '@credo-ts/core'
import type { OutboundPackage } from '@credo-ts/core'

import { emitStructured, makeSpanId, monoNow, durationMs, tryExtractJweFp } from '../logger/StructuredLogger'
import { requestContext } from '../instrumentation/requestContext'

export class InstrumentedWsOutboundTransport extends WsOutboundTransport {
  async sendMessage(outboundPackage: OutboundPackage): Promise<void> {
    const spanId = makeSpanId()
    const startMono = monoNow()
    const targetUrl = outboundPackage.endpoint ?? ''
    // jwe_fp: fingerprint of the outgoing (inner) JWE being delivered to mobile
    // jwe_fp_in: fingerprint of the outer JWE that arrived at the mediator (via ALS)
    const jweFp = tryExtractJweFp(outboundPackage.payload)
    const jweFpIn = requestContext.getStore()?.jweFpIn ?? ''

    // WsOutboundTransport is only used for mobile recipients (never for the controller,
    // which uses HttpOutboundTransport). Every sendMessage here is a live-delivery decision.
    emitStructured(LogLevel.info, {
      hop: 'mediator.forward.strategy.decision',
      conn_id: outboundPackage.connectionId ?? '',
      jwe_fp: jweFp,
      jwe_fp_in: jweFpIn,
      decision: 'live',
    })

    emitStructured(LogLevel.info, {
      hop: 'mediator.live.delivery.start',
      span_id: spanId,
      jwe_fp: jweFp,
      jwe_fp_in: jweFpIn,
      conn_id: outboundPackage.connectionId ?? '',
      target_url: targetUrl,
    })

    try {
      await super.sendMessage(outboundPackage)
      emitStructured(LogLevel.info, {
        hop: 'mediator.live.delivery.end',
        span_id: spanId,
        jwe_fp: jweFp,
        jwe_fp_in: jweFpIn,
        conn_id: outboundPackage.connectionId ?? '',
        target_url: targetUrl,
        duration_ms: durationMs(startMono),
        status: 'ok',
      })
    } catch (err) {
      emitStructured(LogLevel.info, {
        hop: 'mediator.live.delivery.end',
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
