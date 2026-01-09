Feature: Graceful shutdown

  Scenario: Several messages in flight
    Given Several messages are sent to the SQS queue
    Then the application is stopped while messages are in flight
    Then the in-flight messages should be processed before stopped is emitted

  Scenario: Several messages in flight with fastq mode
    Given Several messages are sent to the SQS queue for fastq
    Then the fastq application is stopped while messages are in flight
    Then the in-flight fastq messages should be processed before stopped is emitted