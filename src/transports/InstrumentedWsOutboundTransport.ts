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

    // NOTE: this fires only when Credo opens a NEW outbound WS to a recipient endpoint.
    // It does NOT fire for LiveMode/pickup delivery to a mobile that connected inbound — that
    // path replies over the existing transport session (MessageSender.sendMessageToSession →
    // session.send), bypassing this transport. So absence of these events does not mean live
    // delivery isn't happening; use the mediator.livemode.session.* / transport.session.*
    // events (wired in agent.ts) to observe LiveMode instead.
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
