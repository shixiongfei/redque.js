/*
 * DelayQueue.ts
 *
 * Copyright (c) 2022-2024 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/redque.js
 */

import { ulid } from "ulid";
import { BaseRedis, RedisClientType } from "./BaseRedis.js";

export class DelayQueue extends BaseRedis {
  private static PRODUCER_SCRIPT_SHA = "REDQUE:SCRIPTS:DELAY:PRODUCER:SHA";
  private static CONSUMER_SCRIPT_SHA = "REDQUE:SCRIPTS:DELAY:CONSUMER:SHA";
  private readonly name: string;

  constructor(name: string, redisOrUrl: RedisClientType | string) {
    super(redisOrUrl);
    this.name = name;
  }

  async produce<T>(payload: T, delay: number) {
    let scriptSha = await this.redis.get(DelayQueue.PRODUCER_SCRIPT_SHA);

    if (!scriptSha) {
      scriptSha = await this._reloadProducerScript();
    }

    const args = {
      keys: [this.name, `${this.name}:hash`],
      arguments: [`${delay}`, ulid(), JSON.stringify(payload)],
    };

    try {
      await this.redis.evalSha(scriptSha, args);
    } catch (_) {
      scriptSha = await this._reloadProducerScript();
      await this.redis.evalSha(scriptSha, args);
    }
  }

  async consume<T>(maxDelay: number) {
    let scriptSha = await this.redis.get(DelayQueue.CONSUMER_SCRIPT_SHA);

    if (!scriptSha) {
      scriptSha = await this._reloadConsumerScript();
    }

    const args = {
      keys: [this.name, `${this.name}:hash`],
      arguments: ["0", `${maxDelay}`, "0", "1"],
    };

    let payload: string[];

    try {
      payload = (await this.redis.evalSha(scriptSha, args)) as string[];
    } catch (_) {
      scriptSha = await this._reloadConsumerScript();
      payload = (await this.redis.evalSha(scriptSha, args)) as string[];
    }

    return payload && payload.length > 0
      ? (JSON.parse(payload[0]) as T)
      : undefined;
  }

  private async _reloadScript(key: string, script: string) {
    const scriptSha = await this.redis.scriptLoad(script);
    await this.redis.set(key, scriptSha);
    return scriptSha;
  }

  private async _reloadProducerScript() {
    const script = `
      redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
      redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
      return 1`;
    return await this._reloadScript(DelayQueue.PRODUCER_SCRIPT_SHA, script);
  }

  private async _reloadConsumerScript() {
    const script = `
      local status, type = next(redis.call('TYPE', KEYS[1]))
      if status ~= nil and status == 'ok' then
        if type == 'zset' then
            local list = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'LIMIT', ARGV[3], ARGV[4])
            if list ~= nil and #list > 0 then
                redis.call('ZREM', KEYS[1], unpack(list))
                local result = redis.call('HMGET', KEYS[2], unpack(list))
                redis.call('HDEL', KEYS[2], unpack(list))
                return result
            end
        end
      end
      return nil`;
    return await this._reloadScript(DelayQueue.CONSUMER_SCRIPT_SHA, script);
  }
}
