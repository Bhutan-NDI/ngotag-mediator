import { AskarModule, AskarMultiWalletDatabaseScheme } from '@credo-ts/askar'
import {
  Agent,
  CacheModule,
  ConnectionsModule,
  DidCommMimeType,
  InMemoryLruCache,
  LogLevel,
  MediatorModule,
  OutOfBandRole,
  OutOfBandState,
  WalletConfig,
} from '@credo-ts/core'
import { HttpInboundTransport, WsInboundTransport, agentDependencies } from '@credo-ts/node'
import { ariesAskar } from '@hyperledger/aries-askar-nodejs'
import type { Socket } from 'net'

import express from 'express'
import { Server } from 'ws'

import { AGENT_ENDPOINTS, AGENT_NAME, AGENT_PORT, LOG_LEVEL, POSTGRES_HOST, WALLET_KEY, WALLET_NAME, MESSAGE_FORWARDING_STRATEGY, WALLET_DB_MAX_CONNECTIONS, WALLET_DB_MIN_CONNECTIONS, WALLET_DB_IDLE_TIMEOUT, WALLET_DB_CONNECT_TIMEOUT, USE_PUSH_NOTIFICATIONS } from './constants'
import { askarPostgresConfig } from './database'
import { Logger } from './logger'
import { emitStructured, makeSpanId, monoNow, tryExtractRecipientKeyShort, tryExtractJweFp } from './logger/StructuredLogger'
import { requestContext } from './instrumentation/requestContext'
import { StorageMessageQueueModule } from './storage/StorageMessageQueueModule'
import { PushNotificationsFcmModule } from './push-notifications/fcm'
import { InstrumentedHttpOutboundTransport } from './transports/InstrumentedHttpOutboundTransport'
import { InstrumentedWsOutboundTransport } from './transports/InstrumentedWsOutboundTransport'
import { startGauges } from './instrumentation/gauges'
import { wsSessionOpened, wsSessionClosed, registerQueueAccessor } from './instrumentation/metrics'
import { InjectionSymbols } from '@credo-ts/core'
import { StorageServiceMessageQueue } from './storage/StorageMessageQueue'
import { registerAdminEndpoints, wireQueueDrain } from './instrumentation/adminEndpoint'
import { MessageForwardingStrategy } from '@credo-ts/core/build/modules/routing/MessageForwardingStrategy'

function getForwardingStrategy(): MessageForwardingStrategy {
  const logger = new Logger(LOG_LEVEL)

  const strategy = MESSAGE_FORWARDING_STRATEGY;

  if (!strategy) {
    return MessageForwardingStrategy.DirectDelivery;
  }

  const normalized = strategy.toLowerCase().trim();

  if (normalized === MessageForwardingStrategy.QueueAndLiveModeDelivery.toLowerCase()) {
    return MessageForwardingStrategy.QueueAndLiveModeDelivery;
  }

  if (normalized === MessageForwardingStrategy.QueueOnly.toLowerCase()) {
    return MessageForwardingStrategy.QueueOnly;
  }

  if (normalized === MessageForwardingStrategy.DirectDelivery.toLowerCase()) {
    return MessageForwardingStrategy.DirectDelivery;
  }

  logger.warn(
    `Unknown MEDIATOR_MESSAGE_FORWARDING_STRATEGY "${strategy}". Falling back to DirectDelivery.`
  );
  return MessageForwardingStrategy.DirectDelivery;
}

function createModules() {
  const modules = {
    storageModule: new StorageMessageQueueModule(),
    cache: new CacheModule({
      cache: new InMemoryLruCache({ limit: 500 }),
    }),
    connections: new ConnectionsModule({
      autoAcceptConnections: true,
    }),
    mediator: new MediatorModule({
      autoAcceptMediationRequests: true,
      messageForwardingStrategy: getForwardingStrategy()
    }),
    askar: new AskarModule({
      ariesAskar,
      multiWalletDatabaseScheme: AskarMultiWalletDatabaseScheme.ProfilePerWallet,
    }),
    pushNotificationsFcm: new PushNotificationsFcmModule(),
  }

  return modules
}

export async function createAgent() {
  // We create our own instance of express here. This is not required
  // but allows use to use the same server (and port) for both WebSockets and HTTP
  const app = express()
  registerAdminEndpoints(app)
  const socketServer = new Server({ noServer: true })

  const logger = new Logger(LOG_LEVEL)

  // Only load postgres database in production
  const storageConfig = POSTGRES_HOST ? askarPostgresConfig : undefined

  const walletConfig: WalletConfig = {
    id: WALLET_NAME,
    key: WALLET_KEY,
    storage: storageConfig,
  }

  if (storageConfig) {
    logger.info('Using postgres storage', {
      walletId: walletConfig.id,
      host: storageConfig.config.host,
    })
  } else {
    logger.info('Using SQlite storage', {
      walletId: walletConfig.id,
    })
  }

  const agent = new Agent({
    config: {
      label: AGENT_NAME,
      endpoints: AGENT_ENDPOINTS,
      walletConfig: walletConfig,
      useDidSovPrefixWhereAllowed: true,
      logger: logger,
      // FIXME: We should probably remove this at some point, but it will require custom logic
      // Also, doesn't work with multi-tenancy yet
      autoUpdateStorageOnStartup: true,
      backupBeforeStorageUpdate: false,
      didCommMimeType: DidCommMimeType.V0,
    },
    dependencies: agentDependencies,
    modules: {
      ...createModules(),
    },
  })

  // Create all transports
  const httpInboundTransport = new HttpInboundTransport({ app, port: AGENT_PORT })
  const httpOutboundTransport = new InstrumentedHttpOutboundTransport()
  const wsInboundTransport = new WsInboundTransport({ server: socketServer })
  const wsOutboundTransport = new InstrumentedWsOutboundTransport()

  // HTTP inbound instrumentation — runs after express.text() body parser (added in HttpInboundTransport
  // constructor) so req.body is a string when our middleware fires.
  httpInboundTransport.app.use((req, res, next) => {
    if (req.method === 'POST') {
      const spanId = makeSpanId()
      const rawBody = typeof req.body === 'string' ? req.body : ''
      const recipientKeyShort = rawBody ? tryExtractRecipientKeyShort(rawBody) : ''
      const jweFpIn = rawBody ? tryExtractJweFp(rawBody) : ''
      emitStructured(LogLevel.debug, {
        hop: 'mediator.http.inbound.received',
        span_id: spanId,
        jwe_fp: jweFpIn,
        recipient_key_short: recipientKeyShort,
        content_length: req.headers['content-length'] ? Number(req.headers['content-length']) : undefined,
      })
      res.locals.__dbg_span = spanId
      res.locals.__dbg_start = monoNow()
      // Thread the outer JWE fingerprint through Credo's async processing chain so
      // StorageMessageQueue.addMessage and outbound transports can read jwe_fp_in.
      return requestContext.run({ jweFpIn }, () => next())
    }
    next()
  })

  // WS session instrumentation — add our listener before agent.initialize() registers Credo's listener.
  // We also patch socket.on so that Credo's message handler (registered after agent.initialize())
  // inherits the ALS requestContext that carries the outer JWE fingerprint.
  socketServer.on('connection', (socket) => {
    const sessionId = makeSpanId()
    wsSessionOpened()
    emitStructured(LogLevel.info, {
      hop: 'mediator.ws.session.opened',
      flow: 'lifecycle',
      span_id: sessionId,
      recipient_key_short: '',
      notes: 'recipient_key resolved on first message',
    })
    ;(socket as unknown as Record<string, unknown>)['__dbgSessionId'] = sessionId

    // Patch socket.on so subsequent 'message' listeners (including Credo's) are wrapped in ALS.
    // This must be done before agent.initialize() registers WsInboundTransport's listener.
    const _origOn = socket.on.bind(socket) as typeof socket.on
    ;(socket as unknown as { on: typeof socket.on }).on = (
      event: string,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      listener: (...args: any[]) => void
    ) => {
      if (event === 'message') {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return _origOn(event as 'message', (...args: any[]) => {
          const data = args[0]
          const raw = typeof data === 'string' ? data : Buffer.isBuffer(data) ? data.toString('utf8') : ''
          const jweFpIn = raw ? tryExtractJweFp(raw) : ''
          requestContext.run({ jweFpIn }, () => listener.call(socket, ...args))
        }) as typeof socket
      }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return _origOn(event as any, listener)
    }

    socket.on('message', (data) => {
      const raw = typeof data === 'string' ? data : Buffer.isBuffer(data) ? data.toString('utf8') : ''
      const recipientKeyShort = raw ? tryExtractRecipientKeyShort(raw) : ''
      const jweFpIn = raw ? tryExtractJweFp(raw) : ''
      emitStructured(LogLevel.debug, {
        hop: 'mediator.ws.inbound.received',
        span_id: makeSpanId(),
        jwe_fp: jweFpIn,
        recipient_key_short: recipientKeyShort,
        session_id: sessionId,
        byte_length: raw.length,
      })
    })

    socket.on('close', () => {
      wsSessionClosed()
      emitStructured(LogLevel.info, {
        hop: 'mediator.ws.session.closed',
        flow: 'lifecycle',
        span_id: sessionId,
        recipient_key_short: '',
      })
    })
  })

  // Register all Transports
  agent.registerInboundTransport(httpInboundTransport)
  agent.registerOutboundTransport(httpOutboundTransport)
  agent.registerInboundTransport(wsInboundTransport)
  agent.registerOutboundTransport(wsOutboundTransport)

  // eslint-disable-next-line @typescript-eslint/no-misused-promises
  httpInboundTransport.app.get('/invite', async (req, res) => {
    if (!req.query._oobid || typeof req.query._oobid !== 'string') {
      return res.status(400).send('Missing or invalid _oobid')
    }

    const outOfBandRecord = await agent.oob.findById(req.query._oobid)

    if (
      !outOfBandRecord ||
      outOfBandRecord.role !== OutOfBandRole.Sender ||
      outOfBandRecord.state !== OutOfBandState.AwaitResponse
    ) {
      return res.status(400).send(`No invitation found for _oobid ${req.query._oobid}`)
    }
    return res.send(outOfBandRecord.outOfBandInvitation.toJSON())
  })

  await agent.initialize()

  // Inject agent reference into the admin module so /admin/queue/drain can resolve
  // the message repository at request time.
  wireQueueDrain(agent)

  // Register the queue-depth accessor so the 10s gauge snapshot can include
  // queue_depth_total / queue_oldest_age_ms / queue_depth_top10.
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const queueService = agent.dependencyManager.resolve(InjectionSymbols.MessagePickupRepository as any) as StorageServiceMessageQueue
    registerQueueAccessor(() => queueService.getQueueGaugeSnapshot())
  } catch {
    // DI resolution may fail if the module isn't registered; safe to continue.
  }

  // Wallet pool accessor is intentionally NOT wired today. aries-askar does not
  // expose live postgres pool stats to JS (Rust-side N-API), so the wallet_pool_*
  // gauge fields remain null. Use mediator.queue.write.end.duration_ms growth as
  // an indirect proxy for postgres pool pressure until aries-askar surfaces stats.

  startGauges()

  emitStructured(LogLevel.info, {
    hop: 'mediator.config.dump',
    flow: 'lifecycle',
    notes: 'effective config at startup',
    message_forwarding_strategy: getForwardingStrategy(),
    wallet_db_max_connections: WALLET_DB_MAX_CONNECTIONS,
    wallet_db_min_connections: WALLET_DB_MIN_CONNECTIONS,
    wallet_db_idle_timeout_ms: WALLET_DB_IDLE_TIMEOUT,
    wallet_db_connect_timeout_s: WALLET_DB_CONNECT_TIMEOUT,
    use_push_notifications: USE_PUSH_NOTIFICATIONS,
    postgres_host: POSTGRES_HOST ? POSTGRES_HOST.split(':')[0] : 'sqlite',
    agent_endpoints: AGENT_ENDPOINTS,
  })

  // When an 'upgrade' to WS is made on our http server, we forward the
  // request to the WS server
  httpInboundTransport.server?.on('upgrade', (request, socket, head) => {
    socketServer.handleUpgrade(request, socket as Socket, head, (socket) => {
      socketServer.emit('connection', socket, request)
    })
  })

  return agent
}

export type MediatorAgent = Agent<ReturnType<typeof createModules>>
