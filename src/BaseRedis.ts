/*
 * BaseRedis.ts
 *
 * Copyright (c) 2022 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/redque.js
 */

import { createClient, RedisClientType } from "redis";

export class BaseRedis {
  protected readonly redis: RedisClientType;

  constructor(redisOrUrl: RedisClientType | string) {
    if (typeof redisOrUrl === "string") {
      this.redis = createClient({ url: redisOrUrl });
    } else {
      this.redis = redisOrUrl;
    }
  }

  async connect() {
    if (!this.redis.isOpen && !this.redis.isReady) {
      await this.redis.connect();
    }
  }

  async close() {
    await this.redis.disconnect();
  }
}
