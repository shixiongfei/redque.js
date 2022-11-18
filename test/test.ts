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

import { SimpleQueue, DelayQueue } from "../src/index";

const redisUrl = "redis://:123456@127.0.0.1:6379/1";

type Message = { code: number; payload: string };

const testSimpleQueue = async () => {
  const sq = new SimpleQueue("redque-test-simple-queue", redisUrl);
  await sq.connect();

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    const timestamp = new Date().getTime();
    const message = { code, payload: `Time: ${timestamp}` } as Message;
    await sq.producer(JSON.stringify(message));
    console.log("SimpleQueue producer", message);
  }

  for (;;) {
    const message = await sq.consumer();
    if (!message) break;
    console.log("SimpleQueue consumer", JSON.parse(message));
  }

  await sq.close();
};

const testDelayQueue = async () => {
  const dq = new DelayQueue("redque-test-delay-queue", redisUrl);
  await dq.connect();

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    const timestamp = new Date().getTime();
    const message = { code, payload: `Time: ${timestamp}` } as Message;
    await dq.producer(JSON.stringify(message), code);
    console.log("DelayQueue producer", message);
  }

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    for (;;) {
      const message = await dq.consumer(code);
      if (!message) break;
      console.log("DelayQueue consumer", JSON.parse(message));
    }
  }

  await dq.close();
};

testSimpleQueue();
testDelayQueue();
