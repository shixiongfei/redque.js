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

import { RedisClientType } from "@redis/client";
import { createClient } from "redis";

export class BaseRedis {
  protected readonly redis: RedisClientType;

  constructor(redisUrl: string) {
    this.redis = createClient({ url: redisUrl });
  }

  async connect() {
    return await this.redis.connect();
  }

  async close() {
    return await this.redis.disconnect();
  }
}
