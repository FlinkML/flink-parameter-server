# Flink Parameter Server

A Parameter Server implementation based on the
[Streaming API](https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/datastream_api.html) of [Apache Flink](http://flink.apache.org/).

Parameter Server is an abstraction for model-parallel machine learning training
(see the work of [Li et al.](https://doi.org/10.1145/2640087.2644155)).
Our implementation could be used with the Streaming API:
it can take a `DataStream` of data-points as input, and produce a `DataStream` of model updates.
Currently only asynchronous training is supported.

# Build
Use [SBT](http://www.scala-sbt.org/).

# Docs
See Scala Docs.
