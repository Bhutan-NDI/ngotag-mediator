import {
  AgentContext,
  EventEmitter,
  inject,
  injectable,
  InjectionSymbols,
  JsonTransformer,
  Repository,
  StorageService,
} from '@credo-ts/core'

import { MessageRecord } from './MessageRecord'

type AskarEntry = {
  name: string
  value: string
}

type AskarSession = {
  count(options: { category: string; tagFilter?: Record<string, unknown> }): Promise<number>
  fetchAll(options: { category: string; tagFilter?: Record<string, unknown>; limit?: number }): Promise<AskarEntry[]>
}

type AskarWallet = {
  withSession<Return>(callback: (session: AskarSession) => Return): Promise<Awaited<Return>>
}

@injectable()
export class MessageRepository extends Repository<MessageRecord> {
  public constructor(
    @inject(InjectionSymbols.StorageService)
    storageService: StorageService<MessageRecord>,
    eventEmitter: EventEmitter
  ) {
    super(MessageRecord, storageService, eventEmitter)
  }

  public async countByConnectionId(agentContext: AgentContext, connectionId: string) {
    const wallet = agentContext.wallet as unknown as AskarWallet

    return wallet.withSession((session) =>
      session.count({
        category: MessageRecord.type,
        tagFilter: { connectionId },
      })
    )
  }

  public async findByConnectionId(agentContext: AgentContext, connectionId: string, limit?: number) {
    const wallet = agentContext.wallet as unknown as AskarWallet

    const entries = await wallet.withSession((session) =>
      session.fetchAll({
        category: MessageRecord.type,
        tagFilter: { connectionId },
        limit,
      })
    )

    return entries
      .map((entry) => this.entryToRecord(entry))
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
  }

  // Used by /admin/queue/drain. Askar tag filters don't support range comparisons,
  // so we page through `category` records (capped by `limit`) and filter by createdAt in JS.
  // Returns oldest-first up to `limit`.
  public async findOlderThan(agentContext: AgentContext, cutoffMs: number, limit: number) {
    const wallet = agentContext.wallet as unknown as AskarWallet

    const entries = await wallet.withSession((session) =>
      session.fetchAll({
        category: MessageRecord.type,
        limit,
      })
    )

    return entries
      .map((entry) => this.entryToRecord(entry))
      .filter((record) => record.createdAt.getTime() < cutoffMs)
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
  }

  public async getQueueStats(agentContext: AgentContext): Promise<{
    total: number
    oldestAgeMs: number
    top10: Array<{ connId: string; count: number }>
  }> {
    const wallet = agentContext.wallet as unknown as AskarWallet

    const [total, entries] = await Promise.all([
      wallet.withSession((session) => session.count({ category: MessageRecord.type })),
      // Sample up to 500 records to compute age + per-connection counts in-memory.
      wallet.withSession((session) => session.fetchAll({ category: MessageRecord.type, limit: 500 })),
    ])

    let oldest = Date.now()
    const connCounts: Record<string, number> = {}

    for (const entry of entries) {
      try {
        const record = this.entryToRecord(entry)
        if (record.createdAt.getTime() < oldest) oldest = record.createdAt.getTime()
        const connId = record.connectionId ?? 'unknown'
        connCounts[connId] = (connCounts[connId] ?? 0) + 1
      } catch {
        // skip malformed records
      }
    }

    const oldestAgeMs = entries.length > 0 ? Date.now() - oldest : 0
    const top10 = Object.entries(connCounts)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 10)
      .map(([connId, count]) => ({ connId, count }))

    return { total, oldestAgeMs, top10 }
  }

  private entryToRecord(entry: AskarEntry) {
    const record = JsonTransformer.deserialize(entry.value, MessageRecord)
    record.id = entry.name

    return record
  }
}
