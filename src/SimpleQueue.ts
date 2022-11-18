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

import { BaseRedis } from "./BaseRedis";

export class SimpleQueue extends BaseRedis {
  private readonly name: string;

  constructor(name: string, redisUrl: string) {
    super(redisUrl);
    this.name = name;
  }

  async producer(message: string) {
    await this.redis.lPush(this.name, message);
  }

  async consumer() {
    return this.redis.rPop(this.name);
  }
}
