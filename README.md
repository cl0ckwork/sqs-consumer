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

- The queue is polled continuously for messages using [long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html).
- Throwing an error (or returning a rejected promise) from the handler function will cause the message to be left on the queue. An [SQS redrive policy](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html) can be used to move messages that cannot be processed to a dead letter queue.
- By default messages are processed one at a time – a new message won't be received until the first one has been processed. To process messages in parallel, you have two options:
  - **Legacy Mode**: Use the `batchSize` option [detailed here](https://bbc.github.io/sqs-consumer/interfaces/ConsumerOptions.html#batchSize)
  - **FastQ Mode** (Recommended): Use `processConcurrentMessages: true` and `concurrency` for controlled concurrent processing (see [Concurrent Message Processing](#concurrent-message-processing) section below)
  - It's also important to await any processing that you are doing to ensure that messages are processed one at a time.
- By default, messages that are sent to the `handleMessage` and `handleMessageBatch` functions will be considered as processed if they return without an error.
  - To acknowledge individual messages, please return the message that you want to acknowledge if you are using `handleMessage` or the messages for `handleMessageBatch`.
    - To note, returning an empty object or an empty array will be considered an acknowledgement of no message(s) and will result in no messages being deleted. If you would like to change this behaviour, please use the `alwaysAcknowledge` option [detailed here](https://bbc.github.io/sqs-consumer/interfaces/ConsumerOptions.html).
    - By default, if an object or an array is not returned, all messages will be acknowledged.
- Messages are deleted from the queue once the handler function has completed successfully (the above items should also be taken into account).

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
  processConcurrentMessages: true,  // Enable fastq processing
  concurrency: 5,          // Process max 5 messages simultaneously
  batchSize: 10,          // Fetch up to 10 messages, but process with controlled concurrency
});

app.start();
```

#### Key Benefits of FastQ Mode

- **Controlled Concurrency**: Precisely limit how many messages process simultaneously
- **High Performance**: Built on [fastq](https://www.npmjs.com/package/fastq), faster than Promise.all()
- **Immediate Processing**: New messages start as soon as others complete (no waiting for entire batch)
- **Runtime Updates**: Change concurrency on-the-fly without restarting

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
  batchSize: 10
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

**Legacy Mode (Promise.all)**:
```js
const app = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => { /* process */ },
  batchSize: 5, // All 5 messages start simultaneously
});
// ❌ If one message takes 30s, others wait
// ❌ No granular concurrency control
```

**FastQ Mode (Recommended)**:
```js
const app = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => { /* process */ },
  processConcurrentMessages: true,
  concurrency: 3,    // Exactly 3 messages process concurrently
  batchSize: 10,    // Fetch more, process with control
});
// ✅ New messages start immediately as others complete
// ✅ Consistent resource utilization
// ✅ Better throughput for mixed processing times
```

#### Migration from Legacy Mode

Existing code continues to work unchanged. To upgrade to FastQ mode:

```js
// Before (Legacy - still works)
const app = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => { /* process */ },
  batchSize: 5
});

// After (FastQ - enhanced performance)
const app = Consumer.create({
  queueUrl: "...",
  handleMessage: async (message) => { /* process */ },
  processConcurrentMessages: true,  // Add this
  concurrency: 5,          // Add this (replace batchSize for concurrency control)
  batchSize: 10           // Optional: fetch more messages for better throughput
});
```

#### Configuration Validation

TypeScript enforces correct configuration at compile-time:

```js
// ✅ Valid: Legacy mode
const legacyConsumer = Consumer.create({
  queueUrl: "...",
  handleMessage: async () => {},
  batchSize: 5
});

// ✅ Valid: FastQ mode with both options
const fastqConsumer = Consumer.create({
  queueUrl: "...",
  handleMessage: async () => {},
  processConcurrentMessages: true,
  concurrency: 3
});

// ❌ TypeScript Error: concurrency requires processConcurrentMessages
const invalid = Consumer.create({
  queueUrl: "...",
  handleMessage: async () => {},
  concurrency: 3  // Error: processConcurrentMessages must be true
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
