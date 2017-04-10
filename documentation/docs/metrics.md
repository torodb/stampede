<h1>Metrics</h1>

ToroDB Stampede exposes multiple metrics using JMX, some of them are custom metrics and other are metrics offered by third party products like Flexy-pool. 

## Flexy-pool metrics

ToroDB Stampede uses Hikari as a connection pool, but it is wrapped with Flexy-pool, so the metrics exposed by Flexy-pool are available through JMX. So if a JMX console is used the following metrics are available.


| Name | Description |
|------|-------------|
| concurrentConnectionsHistogram | A histogram of the number of concurrent connections. This indicates how many connections are being used at once. |
| concurrentConnectionRequestsHistogram | A histogram of the number of concurrent connection requests. This indicates how many connection are being requested at once. |
| connectionAcquireMillis | A time histogram for the target data source connection acquire interval. |
| connectionLeaseMillis | A time histogram for the connection lease time. The lease time is the duration between the moment a connection is acquired and the time it gets released. |
| maxPoolSizeHistogram | A histogram of the target pool size. The pool size might change if the IncrementPoolOnTimeoutConnectionAcquiringStrategy is being used. |
| overallConnectionAcquireMillis | A time histogram for the total connection acquire interval. This is the connectionAcquireMillis plus the time spent by the connection acquire strategies. |
| overflowPoolSizeHistogram | A histogram of the pool size overflowing. The pool size might overflow if the IncrementPoolOnTimeoutConnectionAcquiringStrategy is being used. |
| retryAttemptsHistogram | A histogram of the retry attempts number. This is incremented by the RetryConnectionAcquiringStrategy. |

Because ToroDB Stampede uses more than one connection pool, multiple space names will be avaible through the JMX console. 

| Spacename | Description |
|-----------|-------------|
| com.vladmihalcea.flexypool.metric.codehale.JmxMetricReporter.cursors | Read only connections used by the system. |
| com.vladmihalcea.flexypool.metric.codehale.JmxMetricReporter.session | Connections used by the system to do the replication process from the MongoDB instance. |
| com.vladmihalcea.flexypool.metric.codehale.JmxMetricReporter.system | Connections used by the system to do internal operations.  |

More information can be found in this [link](https://github.com/vladmihalcea/flexy-pool)