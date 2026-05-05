import {
  AgentContext,
  EventEmitter,
  inject,
  injectable,
  InjectionSymbols,
  Repository,
  StorageService,
} from '@credo-ts/core'

import { MessageRecord } from './MessageRecord'

@injectable()
export class MessageRepository extends Repository<MessageRecord> {
  public constructor(
    @inject(InjectionSymbols.StorageService)
    storageService: StorageService<MessageRecord>,
    eventEmitter: EventEmitter
  ) {
    super(MessageRecord, storageService, eventEmitter)
  }

  public async findByConnectionId(agentContext: AgentContext, connectionId: string) {
    const records = await this.findByQuery(agentContext, { connectionId })
    return records.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());
  }
}
