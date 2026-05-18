import { JsonTransformer } from '@credo-ts/core'

import { MessageRecord } from '../src/storage/MessageRecord'
import { MessageRepository } from '../src/storage/MessageRepository'

const createRepository = () => new MessageRepository({} as never, {} as never)

const createAgentContext = (session: Record<string, jest.Mock>) =>
  ({
    wallet: {
      withSession: jest.fn((callback) => callback(session)),
    },
  } as never)

const createEntry = (id: string, createdAt: Date) => {
  const messageRecord = new MessageRecord({
    id,
    connectionId: 'connection-id',
    createdAt,
    message: {
      protected: 'protected',
      iv: 'iv',
      ciphertext: 'ciphertext',
      tag: 'tag',
    },
  })

  return {
    name: id,
    value: JsonTransformer.serialize(messageRecord),
  }
}

describe('MessageRepository', () => {
  it('counts queued messages without loading message records', async () => {
    const repository = createRepository()
    const session = {
      count: jest.fn().mockResolvedValue(3),
    }
    const agentContext = createAgentContext(session)

    await expect(repository.countByConnectionId(agentContext, 'connection-id')).resolves.toBe(3)

    expect(session.count).toHaveBeenCalledWith({
      category: MessageRecord.type,
      tagFilter: { connectionId: 'connection-id' },
    })
  })

  it('limits queue fetches and keeps returned messages ordered by creation time', async () => {
    const repository = createRepository()
    const newerMessage = createEntry('newer-message', new Date('2024-01-02T00:00:00.000Z'))
    const olderMessage = createEntry('older-message', new Date('2024-01-01T00:00:00.000Z'))
    const session = {
      fetchAll: jest.fn().mockResolvedValue([newerMessage, olderMessage]),
    }
    const agentContext = createAgentContext(session)

    const records = await repository.findByConnectionId(agentContext, 'connection-id', 10)

    expect(session.fetchAll).toHaveBeenCalledWith({
      category: MessageRecord.type,
      tagFilter: { connectionId: 'connection-id' },
      limit: 10,
    })
    expect(records.map((record) => record.id)).toEqual(['older-message', 'newer-message'])
  })
})
