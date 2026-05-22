import { WsOutboundTransport, LogLevel } from '@credo-ts/core'
import type { OutboundPackage } from '@credo-ts/core'

import { emitStructured, makeSpanId, monoNow, durationMs, tryExtractOuterMsgId } from '../logger/StructuredLogger'

export class InstrumentedWsOutboundTransport extends WsOutboundTransport {
  async sendMessage(outboundPackage: OutboundPackage): Promise<void> {
    const spanId = makeSpanId()
    const startMono = monoNow()
    const targetUrl = outboundPackage.endpoint ?? ''
    const outerMsgId = tryExtractOuterMsgId(outboundPackage.payload)

    // WsOutboundTransport is only used for mobile recipients (never for the controller,
    // which uses HttpOutboundTransport). Every sendMessage here is a live-delivery decision.
    emitStructured(LogLevel.info, {
      hop: 'mediator.forward.strategy.decision',
      conn_id: outboundPackage.connectionId ?? '',
      outer_msg_id: outerMsgId,
      decision: 'live',
      ...(outerMsgId === '' && { notes: 'outer_msg_id not found in protected header' }),
    })

    emitStructured(LogLevel.info, {
      hop: 'mediator.live.delivery.start',
      span_id: spanId,
      outer_msg_id: outerMsgId,
      conn_id: outboundPackage.connectionId ?? '',
      target_url: targetUrl,
    })

    try {
      await super.sendMessage(outboundPackage)
      emitStructured(LogLevel.info, {
        hop: 'mediator.live.delivery.end',
        span_id: spanId,
        outer_msg_id: outerMsgId,
        conn_id: outboundPackage.connectionId ?? '',
        target_url: targetUrl,
        duration_ms: durationMs(startMono),
        status: 'ok',
      })
    } catch (err) {
      emitStructured(LogLevel.info, {
        hop: 'mediator.live.delivery.end',
        span_id: spanId,
        outer_msg_id: outerMsgId,
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
