import { LogLevel } from '@credo-ts/core'
import type { Express, Request, Response } from 'express'
import express from 'express'

import { ADMIN_TOKEN } from '../constants'
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
}
