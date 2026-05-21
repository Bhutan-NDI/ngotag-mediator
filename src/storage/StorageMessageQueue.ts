import type {
  AddMessageOptions,
  GetAvailableMessageCountOptions,
  QueuedMessage,
  RemoveMessagesOptions,
  TakeFromQueueOptions,
  MessagePickupRepository,
} from '@credo-ts/core'

import { injectable, AgentContext, utils, LogLevel } from '@credo-ts/core'

import { MessageRecord } from './MessageRecord'
import { MessageRepository } from './MessageRepository'
import { PushNotificationsFcmRepository } from '../push-notifications/fcm/repository'
import { NOTIFICATION_WEBHOOK_URL, USE_PUSH_NOTIFICATIONS } from '../constants'
import { emitStructured, makeSpanId, monoNow, durationMs } from '../logger/StructuredLogger'
import { recordQueueWrite } from '../instrumentation/metrics'
import fetch from 'node-fetch'

export interface NotificationMessage {
  messageType: string
  token: string
  clientCode: string
}

@injectable()
export class StorageServiceMessageQueue implements MessagePickupRepository {
  private messageRepository: MessageRepository
  private agentContext: AgentContext
  private pushNotificationsFcmRepository: PushNotificationsFcmRepository

  public constructor(
    messageRepository: MessageRepository,
    agentContext: AgentContext,
    pushNotificationsFcmRepository: PushNotificationsFcmRepository
  ) {
    this.messageRepository = messageRepository
    this.agentContext = agentContext
    this.pushNotificationsFcmRepository = pushNotificationsFcmRepository
  }

  public async getAvailableMessageCount(options: GetAvailableMessageCountOptions) {
    const { connectionId } = options

    return this.messageRepository.countByConnectionId(this.agentContext, connectionId)
  }

  public async takeFromQueue(options: TakeFromQueueOptions): Promise<QueuedMessage[]> {
    const { connectionId, limit, deleteMessages } = options

    if (limit === 0) {
      return []
    }

    const spanId = makeSpanId()
    const startMono = monoNow()
    emitStructured(LogLevel.info, {
      hop: 'mediator.pickup.batch.dispatch.start',
      flow: 'pickup',
      span_id: spanId,
      conn_id: connectionId,
      pickup_limit: limit,
    })

    const messageRecords = await this.messageRepository.findByConnectionId(this.agentContext, connectionId, limit)

    this.agentContext.config.logger.debug(
      `Taking ${messageRecords.length} messages from queue for connection ${connectionId} with deleteMessages=${String(
        deleteMessages
      )}`
    )

    if (deleteMessages) {
      this.removeMessages({ connectionId, messageIds: messageRecords.map((msg) => msg.id) })
    }

    const queuedMessages = messageRecords.map((messageRecord) => ({
      id: messageRecord.id,
      receivedAt: messageRecord.createdAt,
      encryptedMessage: messageRecord.message,
    }))

    emitStructured(LogLevel.info, {
      hop: 'mediator.pickup.batch.dispatch.end',
      flow: 'pickup',
      span_id: spanId,
      conn_id: connectionId,
      duration_ms: durationMs(startMono),
      message_count: queuedMessages.length,
      delete_messages: deleteMessages ?? false,
    })

    return queuedMessages
  }

  public async addMessage(options: AddMessageOptions) {
    const { connectionId, payload, messageType } = options

    this.agentContext.config.logger.debug(
      `Adding message to queue for connection ${connectionId} with payload ${JSON.stringify(payload)}`
    )

    // Log the forward strategy decision: this method is called only when queuing is chosen.
    emitStructured(LogLevel.info, {
      hop: 'mediator.forward.strategy.decision',
      flow: 'verification',
      conn_id: connectionId,
      outer_msg_id: '',
      decision: 'queue',
    })

    const spanId = makeSpanId()
    const startMono = monoNow()
    emitStructured(LogLevel.info, {
      hop: 'mediator.queue.write.start',
      flow: 'verification',
      span_id: spanId,
      conn_id: connectionId,
      outer_msg_id: '',
    })

    const id = utils.uuid()

    await this.messageRepository.save(
      this.agentContext,
      new MessageRecord({
        id,
        connectionId,
        message: payload,
      })
    )

    const queueDepth = await this.messageRepository.countByConnectionId(this.agentContext, connectionId)
    recordQueueWrite()
    emitStructured(LogLevel.info, {
      hop: 'mediator.queue.write.end',
      flow: 'verification',
      span_id: spanId,
      conn_id: connectionId,
      outer_msg_id: '',
      duration_ms: durationMs(startMono),
      queue_depth_after: queueDepth,
    })

    // Send a notification to the device
    if (USE_PUSH_NOTIFICATIONS && NOTIFICATION_WEBHOOK_URL) {
      void this.sendNotification(this.agentContext, connectionId, messageType)
    }

    return id
  }

  public async removeMessages(options: RemoveMessagesOptions) {
    const { messageIds } = options

    const deletePromises = messageIds.map((messageId) =>
      this.messageRepository.deleteById(this.agentContext, messageId)
    )

    await Promise.all(deletePromises)
  }

  private async sendNotification(agentContext: AgentContext, connectionId: string, messageType?: string) {
    try {
      // Get the device token for the connection
      const pushNotificationFcmRecord = await this.pushNotificationsFcmRepository.findSingleByQuery(agentContext, {
        connectionId,
      })

      if (!pushNotificationFcmRecord?.deviceToken) {
        this.agentContext.config.logger.info(`No device token found for connectionId so skip sending notification`)
        return
      }

      // Prepare a message to be sent to the device
      const message: NotificationMessage = {
        messageType: messageType || 'default',
        token: pushNotificationFcmRecord?.deviceToken || '',
        clientCode: pushNotificationFcmRecord?.clientCode || '',
      }

      this.agentContext.config.logger.info(`Sending notification to ${pushNotificationFcmRecord?.connectionId}`)
      const pushSpanId = makeSpanId()
      const pushStart = monoNow()
      emitStructured(LogLevel.info, {
        hop: 'mediator.push.send.start',
        flow: 'verification',
        span_id: pushSpanId,
        conn_id: connectionId,
      })
      await this.processNotification(message)
      emitStructured(LogLevel.info, {
        hop: 'mediator.push.send.end',
        flow: 'verification',
        span_id: pushSpanId,
        conn_id: connectionId,
        duration_ms: durationMs(pushStart),
      })
      this.agentContext.config.logger.info(`Notification sent successfully to ${connectionId}`)
    } catch (error) {
      this.agentContext.config.logger.error(`Error sending notification`, {
        cause: error,
      })
    }
  }

  private async processNotification(message: NotificationMessage) {
    try {
      const body = {
        fcmToken: message.token || 'abc',
        messageType: message.messageType,
        clientCode: message.clientCode || '5b4d6bc6-362e-4f53-bdad-ee2742bc0de3',
      }
      const requestOptions = {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      }

      const response = await fetch(NOTIFICATION_WEBHOOK_URL, requestOptions)

      if (response.ok) {
        this.agentContext.config.logger.info(`Notification sent successfully`)
      } else {
        this.agentContext.config.logger.error(`Error sending notification`, {
          cause: response.statusText,
        })
      }
    } catch (error) {
      this.agentContext.config.logger.error(`Error sending notification`, {
        cause: error,
      })
    }
  }
}
