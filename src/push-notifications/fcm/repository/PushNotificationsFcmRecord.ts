import type { TagsBase } from '@aries-framework/core'

import { utils, BaseRecord } from '@aries-framework/core'

export type DefaultPushNotificationsFcmTags = {
  deviceToken: string | null
  devicePlatform: string | null
  connectionId: string
}

export type CustomPushNotificationsFcmTags = TagsBase

export interface PushNotificationsFcmStorageProps {
  id?: string
  deviceToken: string | null
  devicePlatform: string | null
  connectionId: string
  tags?: CustomPushNotificationsFcmTags
}

export class PushNotificationsFcmRecord extends BaseRecord<
  DefaultPushNotificationsFcmTags,
  CustomPushNotificationsFcmTags
> {
  public deviceToken!: string | null
  public devicePlatform!: string | null
  public connectionId!: string

  public static readonly type = 'PushNotificationsFcmRecord'
  public readonly type = PushNotificationsFcmRecord.type

  public constructor(props: PushNotificationsFcmStorageProps) {
    super()

    if (props) {
      this.id = props.id ?? utils.uuid()
      this.devicePlatform = props.devicePlatform
      this.deviceToken = props.deviceToken
      this._tags = props.tags ?? {}
    }
  }

  public getTags() {
    return {
      ...this._tags,
      deviceToken: this.deviceToken,
      devicePlatform: this.devicePlatform,
      connectionId: this.connectionId,
    }
  }
}
