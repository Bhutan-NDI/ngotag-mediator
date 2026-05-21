import { LogLevel } from '@credo-ts/core'

let _level: LogLevel = LogLevel.info

export function getDebugLogLevel(): LogLevel {
  return _level
}

export function setDebugLogLevel(level: LogLevel): void {
  _level = level
}
