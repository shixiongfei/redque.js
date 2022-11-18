import { ulid } from "ulid";
import { BaseRedis } from "./BaseRedis";

export class DelayQueue extends BaseRedis {
  private static PRODUCER_SCRIPT_SHA = "REDQUE:SCRIPTS:DELAY:PRODUCER:SHA";
  private static CONSUMER_SCRIPT_SHA = "REDQUE:SCRIPTS:DELAY:CONSUMER:SHA";
  private readonly name: string;

  constructor(name: string, redisUrl: string) {
    super(redisUrl);
    this.name = name;
  }

  async producer(message: string, score: number) {
    let scriptSha = await this.redis.get(DelayQueue.PRODUCER_SCRIPT_SHA);

    if (!scriptSha) {
      const script = `
        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
        redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
        return 1`;
      scriptSha = await this.redis.scriptLoad(script);
      await this.redis.set(DelayQueue.PRODUCER_SCRIPT_SHA, scriptSha);
    }

    return await this.redis.evalSha(scriptSha, {
      keys: [this.name, `${this.name}:hash`],
      arguments: [`${score}`, ulid(), message],
    });
  }

  async consumer(maxScore: number, count = 10) {
    let scriptSha = await this.redis.get(DelayQueue.CONSUMER_SCRIPT_SHA);

    if (!scriptSha) {
      const script = `
        local status, type = next(redis.call('TYPE', KEYS[1]))
        if status ~= nil and status == 'ok' then
          if type == 'zset' then
              local list = redis.call('ZREVRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'LIMIT', ARGV[3], ARGV[4])
              if list ~= nil and #list > 0 then
                  redis.call('ZREM', KEYS[1], unpack(list))
                  local result = redis.call('HMGET', KEYS[2], unpack(list))
                  redis.call('HDEL', KEYS[2], unpack(list))
                  return result
              end
          end
        end
        return nil`;
      scriptSha = await this.redis.scriptLoad(script);
      await this.redis.set(DelayQueue.CONSUMER_SCRIPT_SHA, scriptSha);
    }

    return await this.redis.evalSha(scriptSha, {
      keys: [this.name, `${this.name}:hash`],
      arguments: [`${maxScore}`, "0", "0", `${count}`],
    });
  }
}
