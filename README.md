# Udacity Data Streaming Project: SF Crime Statistics with Spark Streaming

## How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

I use `maxOffsetsPerTrigger` to impact the throughput and latency of the data.

The higher this value is, the more rows are processed per seconds but also the more delay increases.

I monitor the performance using the progressReport parameters `inputRowsPerSecond`, `processedRowsPerSecond` and `durationMs`, specifically the value of `triggerExecution`

For example,

For a `maxOffsetsPerTrigger` value of 200, i get:
  "inputRowsPerSecond" : 11.667250029168125,
  "processedRowsPerSecond" : 24.786218862312552,
  "durationMs" : {
    "addBatch" : 7964,
    "getBatch" : 6,
    "getOffset" : 5,
    "queryPlanning" : 42,
    "triggerExecution" : 8069,
    "walCommit" : 48
  }

For a `maxOffsetsPerTrigger` value of 1000, i get:
  "inputRowsPerSecond" : 57.23115664167573,
  "processedRowsPerSecond" : 128.0737704918033,
  "durationMs" : {
    "addBatch" : 7689,
    "getBatch" : 8,
    "getOffset" : 10,
    "queryPlanning" : 41,
    "triggerExecution" : 7808,
    "walCommit" : 52
  },


In the above comparison, both batches take about similar time to execute (`triggerExecution` ~ 8s) but one had 200 rows and the other had 1000


## What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

