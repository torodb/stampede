<h1>Off Heap Buffer</h1>

ToroDB can use an off heap replication buffer to store the oplog operations fetched from the remote MongoDB and read it from ToroDB to process it. This way we avoid having to go back to recovery mode when MongoDB has a lot of work.
By default it's disabled because it can take up some extra space on disk, but we recommend to use it if you have many operations on MongoDB.

The recommended configuration is: 

```json   
offHeapBuffer:
  enabled: true
  path: "/tmp/torodb"
  rollCycle: "DAILY"
  maxFiles: 5
```

The [Options Reference](options-reference.md#off-heap-buffer-configuration) explains these settings in detail.