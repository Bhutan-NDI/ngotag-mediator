import { HttpOutboundTransport, LogLevel } from '@credo-ts/core'
import type { OutboundPackage } from '@credo-ts/core'

import { emitStructured, makeSpanId, monoNow, durationMs, tryExtractOuterMsgIdFromPayload } from '../logger/StructuredLogger'
import { recordOutboundMs } from '../instrumentation/metrics'

export class InstrumentedHttpOutboundTransport extends HttpOutboundTransport {
  async sendMessage(outboundPackage: OutboundPackage): Promise<void> {
    const spanId = makeSpanId()
    const startMono = monoNow()
    const targetUrl = outboundPackage.endpoint ?? ''
    const outerMsgId = tryExtractOuterMsgIdFromPayload(outboundPackage.payload)

    emitStructured(LogLevel.info, {
      hop: 'mediator.outbound.send.start',
      span_id: spanId,
      outer_msg_id: outerMsgId,
      conn_id: outboundPackage.connectionId ?? '',
      target_url: targetUrl,
      ...(outerMsgId === '' && { notes: 'outer_msg_id not found in protected header' }),
    })

    try {
      await super.sendMessage(outboundPackage)
      const elapsed = durationMs(startMono)
      recordOutboundMs(elapsed)
      emitStructured(LogLevel.info, {
        hop: 'mediator.outbound.send.end',
        span_id: spanId,
        outer_msg_id: outerMsgId,
        conn_id: outboundPackage.connectionId ?? '',
        target_url: targetUrl,
        duration_ms: elapsed,
        status: 'ok',
      })
    } catch (err) {
      emitStructured(LogLevel.info, {
        hop: 'mediator.outbound.send.end',
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
