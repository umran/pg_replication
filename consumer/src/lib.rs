/// The following is a Kafka consumer that is part of a given consumer_group_id
/// and processes messages from a given set of topics in partition order. In order to ensure idempotency of
/// message processing, the consumer (defined by the consumer_group_id) must keep track of the latest
/// lsn and seq_no processed per topic per partition. In order to preserve idempotency, this state
/// must be read and mutated within the same transaction as any persistent side-effects from processing the message
pub struct Consumer {}
