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
