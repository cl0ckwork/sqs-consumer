# sqs-consumer

[![NPM downloads](https://img.shields.io/npm/dm/sqs-consumer.svg?style=flat)](https://npmjs.org/package/sqs-consumer)
[![Build Status](https://github.com/bbc/sqs-consumer/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/bbc/sqs-consumer/actions/workflows/test.yml)

Build SQS-based applications without the boilerplate. Just define an async function that handles the SQS message processing.

**✨ New in v11.0.0**: Enhanced concurrent processing with [fastq](https://www.npmjs.com/package/fastq) for high-performance message handling with controlled concurrency.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Concurrent Message Processing](#concurrent-message-processing) ✨ **New**
- [FIFO Queue Support](#fifo-queue-support)
- [Credentials](#credentials)
- [AWS IAM Permissions](#aws-iam-permissions)
- [API](#api)
- [Events](#events)
- [Contributing](#contributing)

## Installation

To install this package, simply enter the following command into your terminal (or the variant of whatever package manager you are using):

```bash
npm install sqs-consumer
```

If you would like to use JSR instead, you can find the package [here](https://jsr.io/@bbc/sqs-consumer).

### Node version

We will only support Node versions that are actively or security supported by the Node team. You can find the list of versions that are actively supported [here](https://nodejs.org/en/about/releases/).

## Documentation

Visit [https://bbc.github.io/sqs-consumer/](https://bbc.github.io/sqs-consumer/) for the full API documentation.

## Usage

```js
import { Consumer } from "sqs-consumer";

const app = Consumer.create({
  queueUrl: "https://sqs.eu-west-1.amazonaws.com/account-id/queue-name",
  handleMessage: async (message) => {
    // do some work with `message`
  },
});

app.on("error", (err) => {
  console.error(err.message);
});

app.on("processing_error", (err) => {
  console.error(err.message);
});

app.start();
```

- **Polling Behavior**: The queue is polled continuously for messages using [long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html).
- **Message Processing Behavior**: By default messages are processed one at a time – a new message won't be received until the first one has been processed. To process messages concurrently, you have two options:
  - **Legacy Mode**: Use `batchSize` with `handleMessage` (Promise.all) or `handleMessageBatch` (batch processing) [detailed here](https://bbc.github.io/sqs-consumer/interfaces/ConsumerOptions.html#batchSize)
  - **FastQ Mode** (Recommended): Use `processConcurrentMessages: true` and `concurrency` for controlled concurrent processing (see [Concurrent Message Processing](#concurrent-message-processing) section below)
  - It's also important to await any processing that you are doing to ensure that messages are processed one at a time.
- **Message Acknowledgment Behavior**:
  - When `alwaysAcknowledge` is `false` (the default)
    - **Returning `undefined`, an empty object `{}`, or an empty array `[]`**: Message(s) will **NOT** be deleted (left on queue for retry)
      - For batch processing, return `undefined` or `[]` to prevent acknowledgment of all messages.
      - For single message handling, return `undefined` or `{}` to prevent acknowledgment.
    - **Returning the message object (or an array of messages in batch processing)**: Message(s) will be **acknowledged and deleted**
      - Important: Only the message id(s) that are returned will be deleted.
    - **Returning `void` is discouraged and will be deprecated in a future release.**
    - **When `strictReturn` is `true`:** Returning `null` will throw an error. This will be the default behavior in a future release.
  - **When `alwaysAcknowledge` is `true`:** All messages will be acknowledged and deleted regardless of return value.
- **Error Handling**: Throwing an error (or returning a rejected promise) from the handler function will cause the message to be left on the queue. An [SQS redrive policy](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html) can be used to move messages that cannot be processed to a dead letter queue.
- **Deletion Process** Messages are deleted from the queue once the handler function has completed successfully (the above items should also be taken into account).

### Concurrent Message Processing

By default, sqs-consumer processes messages using `Promise.all()` when `batchSize` is greater than 1. For more sophisticated concurrency control, you can use the built-in **fastq integration** which provides high-performance queue management with precise concurrency limits.

#### FastQ Mode (Recommended for High Throughput)

```js
import { Consumer } from "sqs-consumer";

const app = Consumer.create({
  queueUrl: "https://sqs.eu-west-1.amazonaws.com/account-id/queue-name",
  handleMessage: async (message) => {
    // Process message - this will run with controlled concurrency
    await processMessage(message);
  },
  processConcurrentMessages: true, // Enable fastq processing
  concurrency: 5, // Process max 5 messages simultaneously
  // batchSize defaults to 10 (SQS max) when processConcurrentMessages is true
  // This keeps the internal queue topped off for optimal throughput
});

app.start();
```

**📝 Note**: When `processConcurrentMessages` is enabled, `batchSize` automatically defaults to **10** (the SQS maximum) to ensure the internal fastq queue stays well-fed for optimal throughput. You can override this by explicitly setting `batchSize` to a lower value if desired.

#### Key Benefits of FastQ Mode

- **Controlled Concurrency**: Precisely limit how many messages process simultaneously
- **High Performance**: Built on [fastq](https://www.npmjs.com/package/fastq), faster than Promise.all()
- **Continuous Polling**: New messages are fetched while others are being processed (no batch boundaries)
- **Runtime Updates**: Change concurrency on-the-fly without restarting
- **Optimized Batching**: Automatically fetches 10 messages per poll to keep the queue full

#### How Continuous Polling Works

FastQ mode enables continuous polling, which means new messages are fetched from SQS while previous messages are still being processed:

```js
// With concurrency: 5 and maxInFlightMessages: 15 (default: concurrency * 3)
// Timeline:
// 0s: Fetch 10 messages, start processing 5 (queue: 5 waiting)
// 1s: Continue processing, fetch 10 more (queue: well-fed)
// 2s: Messages complete, new ones start immediately (smooth throughput)
```

This eliminates the "wave" pattern seen in batch processing and maximizes throughput. The `maxInFlightMessages` option (default: `concurrency * 3`) controls how many messages can be queued + processing at once, preventing unbounded memory growth.

#### Runtime Concurrency Updates

```js
// Update concurrency while consumer is running
consumer.updateOption("concurrency", 10);

// Check current concurrency
console.log(consumer.concurrency); // 10
```

**⚠️ Important: Concurrency Update Side Effects**

When updating `concurrency` on a running consumer, the following occurs internally:

1. **Current queue is killed**: All pending (not yet started) messages in the internal queue are discarded
2. **In-flight messages continue**: Messages currently being processed will complete normally
3. **New queue is created**: A fresh fastq queue is initialized with the new concurrency setting
4. **Queue resumes**: If the consumer was actively processing, the new queue starts immediately

```js
// Example: Understanding the impact
const consumer = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => {
    console.log(`Processing: ${message.Body}`);
    await delay(5000); // 5 second processing time
  },
  processConcurrentMessages: true,
  concurrency: 2,
  batchSize: 10,
});

consumer.start();

// After some time, when internal queue might have pending messages
setTimeout(() => {
  // This will:
  // - Kill current queue (discarding 8 pending messages if batchSize=10, concurrency=2)
  // - Let 2 in-flight messages complete
  // - Create new queue with concurrency=5
  consumer.updateOption("concurrency", 5);
}, 10000);
```

**Best Practices for Runtime Updates:**

- **Monitor queue depth**: Consider current load before updating
- **Gradual changes**: Make incremental adjustments rather than large jumps
- **Update during low traffic**: Minimize impact by updating during slower periods
- **Log the changes**: Track concurrency updates for debugging and monitoring

#### Legacy vs FastQ Comparison

**Legacy Mode - Individual Messages (Promise.all)**:

```js
const app = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => {
    /* process each individually */
  },
  batchSize: 5, // Fetches 5, processes with Promise.all()
});
// ❌ All 5 start simultaneously, wait for slowest to complete
// ❌ No granular concurrency control
```

**Legacy Mode - Batch Processing**:

```js
const app = Consumer.create({
  queueUrl: "...",
  handleMessageBatch: async (messages) => {
    /* process batch together */
  },
  batchSize: 5, // Fetches and processes 5 as a single batch
});
// ❌ Entire batch waits for completion before next poll
// ❌ No concurrent processing within batch
```

**FastQ Mode (Recommended)**:

```js
const app = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => {
    /* process */
  },
  processConcurrentMessages: true,
  concurrency: 3, // Exactly 3 messages process concurrently
  batchSize: 10, // Fetch more, process with control
});
// ✅ New messages start immediately as others complete
// ✅ Consistent resource utilization
// ✅ Better throughput for mixed processing times
```

#### Advanced FastQ Configuration

For fine-tuned control over continuous polling behavior:

```js
const app = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => {
    /* process */
  },
  processConcurrentMessages: true,
  concurrency: 5,
  maxInFlightMessages: 20, // Optional: defaults to concurrency * 3
  // Controls max messages in queue + processing
  // Higher = more throughput, more memory
  // Lower = less memory, potential starvation
});
```

**When to adjust `maxInFlightMessages`:**

- **High throughput, fast processing**: Increase to `concurrency * 5` to keep queue full
- **Memory constrained, slow processing**: Decrease to `concurrency * 2` to limit queued messages
- **Default (`concurrency * 3`)**: Good balance for most use cases

#### Migration from Legacy Mode

Existing code continues to work unchanged. To upgrade to FastQ mode:

```js
// Before (Legacy - still works)
const app = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => {
    /* process */
  },
  batchSize: 5,
});

// After (FastQ - enhanced performance)
const app = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => {
    /* process */
  },
  processConcurrentMessages: true, // Add this
  concurrency: 5, // Add this (controls concurrent processing)
  // batchSize automatically defaults to 10 (SQS max) for optimal throughput
});
```

#### Configuration Validation

TypeScript enforces correct configuration at compile-time:

```js
// ✅ Valid: Legacy mode
const legacyConsumer = Consumer.create({
  queueUrl: "...",
  handleMessage: async () => {},
  batchSize: 5,
});

// ✅ Valid: FastQ mode with both options
const fastqConsumer = Consumer.create({
  queueUrl: "...",
  handleMessage: async () => {},
  processConcurrentMessages: true,
  concurrency: 3,
});

// ❌ TypeScript Error: concurrency requires processConcurrentMessages
const invalid = Consumer.create({
  queueUrl: "...",
  handleMessage: async () => {},
  concurrency: 3, // Error: processConcurrentMessages must be true
});
```

### FIFO Queue Support

When using SQS Consumer with FIFO (First-In-First-Out) queues, you might see a warning message in your logs.

As mentioned in the warning, we do not explicitly test SQS Consumer with FIFO queues, this means that we cannot guarantee that the library will work as expected, however, with the correct configuration, it should. If you have done that and believe FIFO to be working as expected, you can suppress the warning by setting `suppressFifoWarning: true`.

To note: In order to maintain FIFO ordering, you should always use the `handleMessageBatch` method instead of `handleMessage`.

### Credentials

By default the consumer will look for AWS credentials in the places [specified by the AWS SDK](https://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html#Setting_AWS_Credentials). The simplest option is to export your credentials as environment variables:

```bash
export AWS_SECRET_ACCESS_KEY=...
export AWS_ACCESS_KEY_ID=...
```

If you need to specify your credentials manually, you can use a pre-configured instance of the [SQS Client](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/classes/sqsclient.html) client.

```js
import { Consumer } from "sqs-consumer";
import { SQSClient } from "@aws-sdk/client-sqs";

const app = Consumer.create({
  queueUrl: "https://sqs.eu-west-1.amazonaws.com/account-id/queue-name",
  handleMessage: async (message) => {
    // ...
  },
  sqs: new SQSClient({
    region: "my-region",
    credentials: {
      accessKeyId: "yourAccessKey",
      secretAccessKey: "yourSecret",
    },
  }),
});

app.on("error", (err) => {
  console.error(err.message);
});

app.on("processing_error", (err) => {
  console.error(err.message);
});

app.on("timeout_error", (err) => {
  console.error(err.message);
});

app.start();
```

### AWS IAM Permissions

Consumer will receive and delete messages from the SQS queue. Ensure `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:DeleteMessageBatch`, `sqs:ChangeMessageVisibility` and `sqs:ChangeMessageVisibilityBatch` access is granted on the queue being consumed.

## API

### `Consumer.create(options)`

Creates a new SQS consumer using the [defined options](https://bbc.github.io/sqs-consumer/interfaces/ConsumerOptions.html).

### `consumer.start()`

Start polling the queue for messages.

### `consumer.stop(options)`

Stop polling the queue for messages. [You can find the options definition here](https://bbc.github.io/sqs-consumer/interfaces/StopOptions.html).

By default, the value of `abort` is set to `false` which means pre existing requests to AWS SQS will still be made until they have concluded. If you would like to abort these requests instead, pass the abort value as `true`, like so:

`consumer.stop({ abort: true })`

#### Graceful shutdowns

Calling `consumer.stop()` on its own only prevents new polls from being scheduled. The consumer will emit `stopped` immediately, even if the current long poll or message handler is still running. To wait for the final poll plus any in-flight message processing to finish, set `pollingCompleteWaitTimeMs` to the maximum amount of time you are willing to wait.

- While waiting, the consumer emits `waiting_for_polling_to_complete` every second.
- If the timeout elapses first, `waiting_for_polling_to_complete_timeout_exceeded` fires right before `stopped`.
- For graceful shutdowns keep `abort: false` (the default). Passing `abort: true` cancels the shared `AbortController`, which halts heartbeat extensions and prevents acknowledgements/deletions from finishing.

Here's an example of a graceful shutdown implementation:

```js
const consumer = Consumer.create({
  queueUrl: "https://sqs.eu-west-1.amazonaws.com/account-id/queue-name",
  handleMessage: async (message) => {
    await doWork(message);
    return message;
  },
  pollingCompleteWaitTimeMs: 10_000, // This will allow up to 10s for the last poll + handler
});

const shutdown = (signal) => {
  console.log(`Received ${signal}, waiting for in-flight work...`);
  consumer.stop();

  consumer.once("waiting_for_polling_to_complete", () => {
    console.log("Still processing in-flight messages...");
  });

  consumer.once("waiting_for_polling_to_complete_timeout_exceeded", () => {
    console.warn("Graceful shutdown timed out, continuing shutdown anyway.");
  });

  consumer.once("stopped", () => {
    console.log("Consumer stopped cleanly");
    process.exit(0);
  });
};

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);

consumer.start();
```

### `consumer.status`

Returns the current status of the consumer.

- `isRunning` - `true` if the consumer has been started and not stopped, `false` if was not started or if it was stopped.
- `isPolling` - `true` if the consumer is actively polling, `false` if it is not.

> **Note:**
> This method is not available in versions before v9.0.0 and replaced the method `isRunning` to supply both running and polling states.

### `consumer.updateOption(option, value)`

Updates the provided option with the provided value.

Please note that any update of the option `pollingWaitTimeMs` will take effect only on next polling cycle.

**Updateable Options:**

- `visibilityTimeout`
- `batchSize`
- `waitTimeSeconds`
- `pollingWaitTimeMs`
- `concurrency` (only available when `processConcurrentMessages: true`)

```js
// Update concurrency for fastq-enabled consumers
consumer.updateOption("concurrency", 8);

// Update other options
consumer.updateOption("batchSize", 3);
consumer.updateOption("pollingWaitTimeMs", 1000);
```

**⚠️ Note on `concurrency` updates**: Updating concurrency will recreate the internal fastq queue, which kills pending (not yet started) messages while allowing in-flight messages to complete. See [Runtime Concurrency Updates](#runtime-concurrency-updates) for details.

You can [find out more about this here](https://bbc.github.io/sqs-consumer/classes/Consumer.html#updateOption).

### Events

Each consumer is an [`EventEmitter`](https://nodejs.org/api/events.html) and [emits these events](https://bbc.github.io/sqs-consumer/interfaces/Events.html).

## Contributing

We welcome and appreciate contributions for anyone who would like to take the time to fix a bug or implement a new feature.

But before you get started, [please read the contributing guidelines](https://github.com/bbc/sqs-consumer/blob/main/.github/CONTRIBUTING.md) and [code of conduct](https://github.com/bbc/sqs-consumer/blob/main/.github/CODE_OF_CONDUCT.md).

## License

SQS Consumer is distributed under the Apache License, Version 2.0, see [LICENSE](https://github.com/bbc/sqs-consumer/blob/main/LICENSE) for more information.
