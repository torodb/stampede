<h1>JVM configuration tips</h1>

The default parameters of Java are usually not the best fit for ToroDB Stampede to work at full capacity.

This is simply a guide suggesting better values for your JVM, but depends a lot about your server setup and configuration.

The following settings apply to the Oracle JVM on a dedicates server for ToroDB Stampede.

## Maximum Heap Size (-Xmx)

3/4 of the total server memory is a reasonable value for a server dedicated to ToroDB Stampede.

You can set this value using parameter `-Xmx<bytes>`. The memory flag can also be specified in multiple sizes, such as kilobytes, megabytes, and so on: `-Xmx1024k`, `-Xmx512m`, `-Xmx8g`.

## Initial Heap Size (-Xms)

1/4 of the total server memory is a reasonable value for a server dedicated to ToroDB Stampede.

The memory flag can also be specified in multiple sizes, such as kilobytes, megabytes, and so on: `-Xms1024k`, `-Xms512m`, `-Xms8g`.

## Garbage Collector

For ToroDB Stampede, we suggest different garbage collectors based on the maximum heap size:

* **up to 4GB**  
  We suggest the CMS garbage collector (`-XX:+UseConcMarkSweepGC`)
* **above 4GB**  
  We suggest the G1 garbage collector (`-XX:+UseG1GC`)
