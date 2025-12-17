Feature: FastQ concurrent message processing

  Background:
    Given the SQS queue is empty

  Scenario: Messages are processed with controlled concurrency
    Given 5 messages are sent to the SQS queue
    When the consumer with concurrency 2 processes the messages
    Then all messages should be consumed without error
    And messages should be processed with controlled concurrency

  Scenario: Concurrent processing maintains message integrity
    Given 6 messages are sent to the SQS queue
    When the consumer with concurrency 3 processes the messages
    Then all messages should be consumed without error
    And no messages should be lost or processed multiple times

  Scenario: Concurrency can be updated at runtime
    Given 3 messages are sent to the SQS queue
    When the consumer starts with concurrency 1
    And the concurrency is updated to 2 during processing
    Then all messages should be consumed without error
    And the updated concurrency should be reflected

  Scenario: Legacy mode still works without fastq options
    Given 3 messages are sent to the SQS queue
    When the consumer without fastq options processes the messages
    Then all messages should be consumed without error
    And processing should use legacy Promise.all approach

  Scenario: FastQ mode enables continuous polling for high throughput
    Given 10 messages are sent to the SQS queue
    When the consumer with concurrency 3 processes messages continuously
    Then the consumer should process messages without error
    And messages should be processed with continuous polling behavior