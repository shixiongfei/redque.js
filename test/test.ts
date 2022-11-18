import { DelayQueue } from "../src/index";

const redisUrl = "redis://:123456@127.0.0.1:6379/1";

type Message = { code: number; payload: string };

const testDelayQueue = async () => {
  const dq = new DelayQueue("redque-test-delay-queue", redisUrl);
  await dq.connect();

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    const message = { code, payload: `Hello ${code}` } as Message;
    await dq.producer(JSON.stringify(message), code);
    console.log("DelayQueue producer", message);
  }

  for (const code of [10, 20, 30, 40, 50, 60, 70, 80, 90]) {
    const messages = await dq.consumer(code);
    if (messages) {
      for (const message of messages) {
        console.log("DelayQueue consumer", JSON.parse(message));
      }
    }
  }

  await dq.close();
};

testDelayQueue();
