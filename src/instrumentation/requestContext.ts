import { AsyncLocalStorage } from 'async_hooks'

export interface MediatorRequestContext {
  jweFpIn: string
}

// Carries the outer JWE fingerprint (`iv`) from the inbound transport into
// StorageMessageQueue.addMessage and outbound transport wrappers, so that
// jwe_fp_in (outer) and jwe_fp_out (inner) can be logged on the same line.
export const requestContext = new AsyncLocalStorage<MediatorRequestContext>()
