/*
 * StreamQueue.ts
 *
 * Copyright (c) 2022 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/redque.js
 */

import { BaseRedis, RedisClientType } from "./BaseRedis";

export class StreamQueue extends BaseRedis {
  private readonly stream: string;

  constructor(stream: string, redisOrUrl: RedisClientType | string) {
    super(redisOrUrl);
    this.stream = stream;
  }

  async producer(payload: unknown, maxLen = 100000) {
    await this.redis.xAdd(
      this.stream,
      "*",
      { payload: JSON.stringify(payload) },
      {
        TRIM: {
          strategy: "MAXLEN",
          strategyModifier: "~",
          threshold: maxLen,
        },
      }
    );
  }

  async ensureGroup(group: string, rewind = false) {
    try {
      await this.redis.xGroupCreate(this.stream, group, rewind ? "0" : "$", {
        MKSTREAM: true,
      });
    } catch (_) {
      // Ignore "BUSYGROUP Consumer Group name already exists"
    }
  }

  async consumer(group: string, consumer: string) {
    return this._consumer(group, consumer, ">");
  }

  async consumerPending(group: string, consumer: string) {
    return this._consumer(group, consumer, "0");
  }

  async ack(group: string, messageId: string) {
    await this.redis.xAck(this.stream, group, messageId);
  }

  private async _consumer(group: string, consumer: string, id: string) {
    const response = await this.redis.xReadGroup(
      group,
      consumer,
      { key: this.stream, id },
      { COUNT: 1 }
    );

    if (!response || response.length === 0) {
      return undefined;
    }

    const entry = response[0];

    if (entry.messages.length === 0) {
      return undefined;
    }

    const { id: messageId, message } = entry.messages[0];
    return [messageId, JSON.parse(message["payload"])] as [string, unknown];
  }
}
