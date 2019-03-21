# Tracing of consumer with custom ConsumerRebalanceListener

Current console-consumer example should cover all use cases and answer those questions

## Use cases (scenarios)

### Case 0
1. consumer poll (lets say 100 records in poll)
2. consumer processed 50 record and failed on 51.

Q: How to commit 50 processed records, start next time from records 51

### Case 1
1. consumer poll
2. processing of current poll takes long time, group coordinator might assume that consumer is dead and trigger a rebalance activity

Q: How to commit latest processed record from last poll ?

### Case 2

1. consumer poll
2. accidental rebalancing by group coordinator (scaling, consumer died)

Q: How to commit latest processed record from last poll ?



## Evaluate approach
// todo: Evaluation of Kafka client configurations via distributed tracing.

Questions:
1. Is it easier use Kafka Streams API to control flow (commit after processing) ?

