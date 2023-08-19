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

  async producer(payload: unknown) {
    await this.redis.lPush(this.name, JSON.stringify(payload));
  }

  async consumer() {
    const payload = await this.redis.rPop(this.name);
    return payload ? JSON.parse(payload) : undefined;
  }
}
