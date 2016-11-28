<h1>JVM configuration tips</h1>

The default parameters of Java are usually not the best fit for ToroDB Stampede to work at full capacity.

This is simply a guide suggesting better values for your JVM, but depends a lot about your server setup and configuration.

## Oracle JVM section

### Memory section

  * **maximum Java heap size**: A reasonable value of 3/4 of total memory in your server. You can set this value using parameter `-Xmx<bytes>` (The memory flag can also be specified in multiple sizes, such as kilobytes, megabytes, and so on: `-Xmx1024k`, `-Xmx512m`, `-Xmx8g`). 
  * **initial Java heap size** (**-Xms**): A reasonable value of 1/4 of total memory in your server. You can set this value using parameter `-Xms<bytes>` (The memory flag can also be specified in multiple sizes, such as kilobytes, megabytes, and so on: `-Xms1024k`, `-Xms512m`, `-Xms8g`).
  
### Garbage collector section
  
  * **garbage collector**: By default the parallel garbage collector is used. For ToroDB Stampede we suggest to use CMS garbage collector (`-XX:+UseConcMarkSweepGC`) for heap up to 4GB and G1 garbage collector for head with more than 4GB  (`-XX:+UseG1GC`). 
