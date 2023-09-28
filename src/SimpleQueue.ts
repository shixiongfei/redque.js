/*
 * SimpleQueue.ts
 *
 * Copyright (c) 2022 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/redque.js
 */

import { BaseRedis, RedisClientType } from "./BaseRedis";

export class SimpleQueue extends BaseRedis {
  private readonly name: string;

  constructor(name: string, redisOrUrl: RedisClientType | string) {
    super(redisOrUrl);
    this.name = name;
  }

  async produce<T>(payload: T) {
    await this.redis.lPush(this.name, JSON.stringify(payload));
  }

  async consume<T>(timeout?: number) {
    return timeout === undefined
      ? await this._consumeImmediate<T>()
      : await this._consumeTimeout<T>(timeout);
  }

  private async _consumeImmediate<T>() {
    const payload = await this.redis.rPop(this.name);
    return payload ? (JSON.parse(payload) as T) : undefined;
  }

  private async _consumeTimeout<T>(timeout: number) {
    const payload = await this.redis.brPop(this.name, timeout);
    return payload ? (JSON.parse(payload.element) as T) : undefined;
  }
}
