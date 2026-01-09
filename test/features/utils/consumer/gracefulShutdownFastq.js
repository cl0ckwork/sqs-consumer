import { Consumer } from "../../../../dist/esm/consumer.js";

import { QUEUE_URL, sqs } from "../sqs.js";

export const fastqConsumer = Consumer.create({
  queueUrl: QUEUE_URL,
  sqs,
  pollingWaitTimeMs: 1000,
  pollingCompleteWaitTimeMs: 5000,
  processConcurrentMessages: true,
  concurrency: 2, // Process 2 messages at a time
  async handleMessage(message) {
    // Simulate slow message processing
    await new Promise((resolve) => setTimeout(resolve, 1500));
    return message;
  },
});
