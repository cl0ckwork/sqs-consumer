import { Consumer } from "../../../../dist/esm/consumer.js";
import { QUEUE_URL, sqs } from "../sqs.js";
import { delay } from "../delay.js";

// Tracking variables for testing concurrency and message processing
export let processedMessages = [];
export let processingTimes = [];
export let maxConcurrentProcessing = 0;
let currentlyProcessing = 0;
export let pollTimestamps = [];
export let firstMessageStartTime = null;

/**
 * Reset all tracking counters for testing.
 * @returns {void}
 */
export function resetCounters() {
  processedMessages = [];
  processingTimes = [];
  maxConcurrentProcessing = 0;
  currentlyProcessing = 0;
  pollTimestamps = [];
  firstMessageStartTime = null;
}

// Create a fastq consumer with specified concurrency
/**
 * Create a fastq consumer with specified concurrency.
 * @param {number} concurrency The concurrency level
 * @param {boolean} shouldThrowError Whether to throw errors
 * @param {number} errorIndex Index at which to throw error
 * @returns {Consumer} A Consumer instance
 */
export function fastqConsumer(
  concurrency = 2,
  shouldThrowError = false,
  errorIndex = -1,
) {
  return Consumer.create({
    queueUrl: QUEUE_URL,
    sqs,
    pollingWaitTimeMs: 50,
    processConcurrentMessages: true,
    concurrency,
    batchSize: 2, // Use smaller batch for testing to get more response_processed events
    async handleMessage(message) {
      const startTime = Date.now();
      currentlyProcessing++;
      maxConcurrentProcessing = Math.max(
        maxConcurrentProcessing,
        currentlyProcessing,
      );

      try {
        // Simulate some processing time to test concurrency
        await delay(100);

        // Check if we should throw an error for this message
        if (shouldThrowError && processedMessages.length === errorIndex) {
          throw new Error(`Simulated error for message ${message.Body}`);
        }

        processedMessages.push(message);
        processingTimes.push({
          messageId: message.MessageId,
          processingTime: Date.now() - startTime,
          concurrentCount: currentlyProcessing,
        });

        return message;
      } finally {
        currentlyProcessing--;
      }
    },
  });
}

// Create a legacy consumer without fastq options
/**
 * Create a legacy consumer without fastq options.
 * @returns {Consumer} A Consumer instance
 */
export function legacyConsumer() {
  return Consumer.create({
    queueUrl: QUEUE_URL,
    sqs,
    pollingWaitTimeMs: 50,
    batchSize: 3, // Test legacy concurrent processing
    async handleMessage(message) {
      const startTime = Date.now();

      // Simulate processing time
      await delay(50);

      processedMessages.push(message);
      processingTimes.push({
        messageId: message.MessageId,
        processingTime: Date.now() - startTime,
        mode: "legacy",
      });

      return message;
    },
  });
}

// Create a consumer that throws errors on specific messages
/**
 * Create a consumer that throws errors on specific messages.
 * @param {number} concurrency The concurrency level
 * @param {number} errorIndex Index at which to throw error
 * @returns {Consumer} A Consumer instance
 */
export function createErrorConsumer(concurrency = 2, errorIndex = 1) {
  return Consumer.create({
    queueUrl: QUEUE_URL,
    sqs,
    pollingWaitTimeMs: 50,
    processConcurrentMessages: true,
    concurrency,
    batchSize: 2, // Use smaller batch for testing
    async handleMessage(message) {
      const startTime = Date.now();
      currentlyProcessing++;
      maxConcurrentProcessing = Math.max(
        maxConcurrentProcessing,
        currentlyProcessing,
      );

      try {
        // Simulate processing time
        await delay(100);

        // Throw error for specific message index
        if (processedMessages.length === errorIndex) {
          throw new Error(`Intentional error for message: ${message.Body}`);
        }

        processedMessages.push(message);
        processingTimes.push({
          messageId: message.MessageId,
          processingTime: Date.now() - startTime,
          concurrentCount: currentlyProcessing,
        });

        return message;
      } finally {
        currentlyProcessing--;
      }
    },
  });
}

// Utility to create a consumer that can be configured for different test scenarios
/**
 * Utility to create a consumer that can be configured for different test scenarios.
 * @param {Object} options Configuration options
 * @returns {Consumer} A Consumer instance
 */
export function createTestConsumer(options = {}) {
  const {
    concurrency = 2,
    processConcurrentMessages = true,
    processingDelay = 100,
    shouldError = false,
    errorIndex = -1,
    batchSize = 1,
  } = options;

  const baseConfig = {
    queueUrl: QUEUE_URL,
    sqs,
    pollingWaitTimeMs: 50,
    batchSize,
    async handleMessage(message) {
      const startTime = Date.now();

      if (processConcurrentMessages) {
        currentlyProcessing++;
        maxConcurrentProcessing = Math.max(
          maxConcurrentProcessing,
          currentlyProcessing,
        );
      }

      try {
        // Simulate processing time
        await delay(processingDelay);

        // Check if we should throw an error
        if (shouldError && processedMessages.length === errorIndex) {
          throw new Error(`Test error for message: ${message.Body}`);
        }

        processedMessages.push(message);
        processingTimes.push({
          messageId: message.MessageId,
          processingTime: Date.now() - startTime,
          concurrentCount: processConcurrentMessages
            ? currentlyProcessing
            : "legacy",
          mode: processConcurrentMessages ? "fastq" : "legacy",
        });

        return message;
      } finally {
        if (processConcurrentMessages) {
          currentlyProcessing--;
        }
      }
    },
  };

  // Add fastq options if enabled
  if (processConcurrentMessages) {
    baseConfig.processConcurrentMessages = true;
    baseConfig.concurrency = concurrency;
  }

  return Consumer.create(baseConfig);
}

// Create a consumer for testing continuous polling behavior
/**
 * Create a consumer for testing continuous polling behavior.
 * @param {number} concurrency The concurrency level
 * @returns {Consumer} A Consumer instance
 */
export function continuousPollingConsumer(concurrency = 3) {
  const consumer = Consumer.create({
    queueUrl: QUEUE_URL,
    sqs,
    pollingWaitTimeMs: 50,
    processConcurrentMessages: true,
    concurrency,
    batchSize: 5, // Use smaller batch to force multiple polls
    async handleMessage(message) {
      const startTime = Date.now();

      // Track first message start time
      if (!firstMessageStartTime) {
        firstMessageStartTime = startTime;
      }

      currentlyProcessing++;
      maxConcurrentProcessing = Math.max(
        maxConcurrentProcessing,
        currentlyProcessing,
      );

      try {
        // Simulate processing time to allow polls to happen during processing
        await delay(200);

        processedMessages.push(message);
        processingTimes.push({
          messageId: message.MessageId,
          processingTime: Date.now() - startTime,
          concurrentCount: currentlyProcessing,
        });

        return message;
      } finally {
        currentlyProcessing--;
      }
    },
  });

  // Track response_processed events to see when polls complete
  consumer.on("response_processed", () => {
    pollTimestamps.push(Date.now());
  });

  return consumer;
}
