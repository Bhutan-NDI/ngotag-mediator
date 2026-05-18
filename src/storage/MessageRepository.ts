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

  private entryToRecord(entry: AskarEntry) {
    const record = JsonTransformer.deserialize(entry.value, MessageRecord)
    record.id = entry.name

    return record
  }
}
