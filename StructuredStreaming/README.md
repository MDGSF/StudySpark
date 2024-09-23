# structured streaming

Batch mode 吞吐量大、延迟高（秒级），而 Continuous mode 吞吐量低、延迟也更低（毫秒级）。

从数据一致性的角度出发，这种容错的能力，可以划分为 3 种水平：

- At most once：最多交付一次，数据存在丢失的风险；
- At least once：最少交付一次，数据存在重复的可能；
- Exactly once：交付且仅交付一次，数据不重不漏。

在数据处理上，结合容错机制，Structured Streaming 本身能够提供“At least once”的处理能力。而结合幂等的 Sink，Structured Streaming 可以实现端到端的“Exactly once”容错水平。

比方说，应用广泛的 Kafka，在 Producer 级别提供跨会话、跨分区的幂等性。结合 Kafka 这样的 Sink，在端到端的处理过程中，Structured Streaming 可以实现“Exactly once”，保证数据的不重不漏。
