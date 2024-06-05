## Proof of Concept: Serving prefixed incrementing unique IDs as fast as possible
This proof of concept demonstrates a service designed to generate prefixed incrementing unique IDs as quickly as possible. The implementation leverages various techniques to ensure both high performance and reliability.

* Implements a write-ahead log (WAL) for crash recovery, ensuring data integrity.
* Maintains counters in memory to provide high throughput, allowing for rapid ID generation.
* Periodically flushes the counters to Cosmos DB to persist the data.
* Utilizes in-process queues to maintain the delivery order for each partition, ensuring that IDs are generated and delivered in sequence.
* Relies on the proxy server to route to the correct node.
