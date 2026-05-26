import type { Agent } from '@credo-ts/core'
import { LogLevel, RecordNotFoundError } from '@credo-ts/core'
import type { Express, Request, Response } from 'express'
import express from 'express'

import { ADMIN_TOKEN } from '../constants'
import { emitStructured, makeSpanId, monoNow, durationMs } from '../logger/StructuredLogger'
import { MessageRepository } from '../storage/MessageRepository'
import { getDebugLogLevel, setDebugLogLevel } from './logLevelHolder'

const LOG_LEVEL_MAP: Record<string, LogLevel> = {
  test: LogLevel.test,
  trace: LogLevel.trace,
  debug: LogLevel.debug,
  info: LogLevel.info,
  warn: LogLevel.warn,
  error: LogLevel.error,
  fatal: LogLevel.fatal,
  off: LogLevel.off,
}

let _agent: Agent | undefined

// Called by agent.ts AFTER agent.initialize() completes — gives the drain handler
// a working agent reference so it can resolve MessageRepository at request time.
export function wireQueueDrain(agent: Agent): void {
  _agent = agent
}

function authMiddleware(req: Request, res: Response, next: () => void): void {
  if (!ADMIN_TOKEN) {
    res.status(503).json({ error: 'admin endpoint disabled (ADMIN_TOKEN not set)' })
    return
  }
  const authHeader = req.headers['authorization']
  if (!authHeader || authHeader !== `Bearer ${ADMIN_TOKEN}`) {
    res.status(401).json({ error: 'unauthorized' })
    return
  }
  next()
}

interface DrainRequestBody {
  olderThanHours?: unknown
  dryRun?: unknown
  maxBatch?: unknown
  connectionIdAllowList?: unknown
}

interface ParsedDrainRequest {
  olderThanHours: number
  dryRun: boolean
  maxBatch: number
  connectionIdAllowList: string[] | null
}

function parseDrainBody(body: DrainRequestBody): ParsedDrainRequest | { error: string } {
  const { olderThanHours, dryRun, maxBatch, connectionIdAllowList } = body

  if (typeof olderThanHours !== 'number' || !Number.isFinite(olderThanHours) || olderThanHours < 1) {
    return { error: 'olderThanHours must be a number >= 1' }
  }
  if (typeof dryRun !== 'boolean') {
    return { error: 'dryRun must be a boolean' }
  }
  let parsedMaxBatch = 500
  if (maxBatch !== undefined) {
    if (typeof maxBatch !== 'number' || !Number.isFinite(maxBatch) || maxBatch < 1 || maxBatch > 5000) {
      return { error: 'maxBatch, if provided, must be a number between 1 and 5000' }
    }
    parsedMaxBatch = Math.floor(maxBatch)
  }
  let parsedAllowList: string[] | null = null
  if (connectionIdAllowList !== undefined && connectionIdAllowList !== null) {
    if (!Array.isArray(connectionIdAllowList) || !connectionIdAllowList.every((c) => typeof c === 'string')) {
      return { error: 'connectionIdAllowList, if provided, must be an array of strings' }
    }
    parsedAllowList = connectionIdAllowList as string[]
  }

  return {
    olderThanHours: Math.floor(olderThanHours),
    dryRun,
    maxBatch: parsedMaxBatch,
    connectionIdAllowList: parsedAllowList,
  }
}

export function registerAdminEndpoints(app: Express): void {
  app.use('/admin', express.json())

  app.get('/admin/log-level', authMiddleware, (_req: Request, res: Response) => {
    const current = getDebugLogLevel()
    const name = Object.entries(LOG_LEVEL_MAP).find(([, v]) => v === current)?.[0] ?? String(current)
    res.json({ level: name, service: 'mediator' })
  })

  app.post('/admin/log-level', authMiddleware, (req: Request, res: Response) => {
    const body = req.body as Record<string, unknown>
    const level = body['level']
    if (typeof level !== 'string' || !(level in LOG_LEVEL_MAP)) {
      res.status(400).json({ error: 'invalid level', valid: Object.keys(LOG_LEVEL_MAP) })
      return
    }
    setDebugLogLevel(LOG_LEVEL_MAP[level])
    res.json({ level, service: 'mediator', ok: true })
  })

  app.post('/admin/queue/drain', authMiddleware, async (req: Request, res: Response) => {
    if (!_agent) {
      res.status(503).json({ error: 'agent not yet initialized; retry shortly' })
      return
    }

    const parsed = parseDrainBody(req.body as DrainRequestBody)
    if ('error' in parsed) {
      res.status(400).json({ error: parsed.error })
      return
    }
    const { olderThanHours, dryRun, maxBatch, connectionIdAllowList } = parsed

    const cutoffMs = Date.now() - olderThanHours * 3600 * 1000
    const cutoffIso = new Date(cutoffMs).toISOString()
    const spanId = makeSpanId()
    const startMono = monoNow()

    emitStructured(LogLevel.info, {
      hop: 'mediator.admin.drain.start',
      span_id: spanId,
      notes: `olderThanHours=${olderThanHours} dryRun=${dryRun} maxBatch=${maxBatch} allowList=${
        connectionIdAllowList ? connectionIdAllowList.length : 'all'
      }`,
    })

    try {
      const agentContext = _agent.context
      const repo = agentContext.dependencyManager.resolve(MessageRepository)

      // connectionIdAllowList filtering and age filtering happen inside findOlderThan,
      // before the maxBatch slice, so allowlisted records are never shadowed by newer ones.
      const candidates = await repo.findOlderThan(agentContext, cutoffMs, maxBatch, connectionIdAllowList)

      const byConnectionMap: Record<string, number> = {}
      for (const r of candidates) {
        byConnectionMap[r.connectionId] = (byConnectionMap[r.connectionId] ?? 0) + 1
      }
      const byConnection = Object.entries(byConnectionMap)
        .map(([connId, count]) => ({ connId, count }))
        .sort((a, b) => b.count - a.count)

      let deleted = 0
      if (!dryRun) {
        for (const r of candidates) {
          try {
            await repo.deleteById(agentContext, r.id)
            deleted++
          } catch (err) {
            // Record already removed by a concurrent takeFromQueue dispatch — safe to skip.
            if (err instanceof RecordNotFoundError) continue
            throw err
          }
        }
      }

      const elapsedMs = durationMs(startMono)
      emitStructured(LogLevel.info, {
        hop: 'mediator.admin.drain.end',
        span_id: spanId,
        duration_ms: elapsedMs,
        notes: `scanned=${candidates.length} deleted=${deleted} dryRun=${dryRun}`,
      })

      res.json({
        service: 'mediator',
        dryRun,
        olderThanHours,
        cutoffIso,
        scanned: candidates.length,
        ...(dryRun
          ? { wouldDelete: candidates.length }
          : { deleted, alreadyGone: candidates.length - deleted }),
        byConnection: dryRun
          ? byConnection.map(({ connId, count }) => ({ connId, wouldDelete: count }))
          : byConnection.map(({ connId, count }) => ({ connId, deleted: count })),
        maxBatch,
        durationMs: elapsedMs,
      })
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      emitStructured(LogLevel.error, {
        hop: 'mediator.admin.drain.end',
        span_id: spanId,
        duration_ms: durationMs(startMono),
        notes: `error: ${message.slice(0, 200)}`,
      })
      res.status(500).json({ error: 'drain failed', message: message.slice(0, 200) })
    }
  })
}
