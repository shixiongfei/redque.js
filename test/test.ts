/*
 * test.ts
 *
 * Copyright (c) 2022 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/redque.js
 */

import { createClient } from "redis";
import { SimpleQueue, DelayQueue, StreamQueue } from "../src/index";

const redisUrl = "redis://:123456@127.0.0.1:6379/1";
const redis = createClient({ url: redisUrl });

type Message = { code: number; payload: string };

const testSimpleQueue = async () => {
  const sq = new SimpleQueue("redque-test-simple-queue", redis);
  await sq.connect();

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    const timestamp = new Date().getTime();
    const message = { code, payload: `Time: ${timestamp}` } as Message;
    await sq.producer(message);
    console.log("SimpleQueue producer", message);
  }

  for (;;) {
    const message = await sq.consumer<Message>();
    if (!message) break;
    console.log("SimpleQueue consumer", message);
  }

  await sq.close();
};

const testDelayQueue = async () => {
  const dq = new DelayQueue("redque-test-delay-queue", redisUrl);
  await dq.connect();

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    const timestamp = new Date().getTime();
    const message = { code, payload: `Time: ${timestamp}` } as Message;
    await dq.producer(message, code);
    console.log("DelayQueue producer", message);
  }

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    for (;;) {
      const message = await dq.consumer<Message>(code);
      if (!message) break;
      console.log("DelayQueue consumer", message);
    }
  }

  await dq.close();
};

const testStreamQueue = async () => {
  const group = "redque-test-stream-group";
  const consumer = "redque-test-stream-group-consumer";
  const sq = new StreamQueue("redque-test-stream-queue", redis);
  await sq.connect();
  await sq.ensureGroup(group);

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    const timestamp = new Date().getTime();
    const message = { code, payload: `Time: ${timestamp}` } as Message;
    await sq.producer(message);
    console.log("StreamQueue producer", message);
  }

  for (;;) {
    const message = await sq.consumer<Message>(group, consumer);
    if (!message) break;
    console.log("StreamQueue consumer", message);
  }

  for (;;) {
    const message = await sq.consumerPending<Message>(group, consumer);
    if (!message) break;
    await sq.ack(group, message[0]);
    console.log("StreamQueue consumer pending", message);
  }

  await sq.close();
};

const test = async () => {
  await redis.connect();
  await Promise.all([testSimpleQueue(), testDelayQueue(), testStreamQueue()]);
  await redis.disconnect();
};

test();
