/*
 * BaseRedis.ts
 *
 * Copyright (c) 2022-2024 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/redque.js
 */

import { createClient } from "redis";

export type RedisClientType = ReturnType<typeof createClient>;

export class BaseRedis {
  protected readonly redis: RedisClientType;
  protected readonly maintenance: boolean;

  constructor(redisOrUrl: RedisClientType | string) {
    if (typeof redisOrUrl === "string") {
      this.redis = createClient({ url: redisOrUrl });
      this.maintenance = true;
    } else {
      this.redis = redisOrUrl;
      this.maintenance = false;
    }
  }

  async connect() {
    if (this.maintenance) {
      await this.redis.connect();
    }
  }

  async close() {
    if (this.maintenance) {
      await this.redis.disconnect();
    }
  }
}
