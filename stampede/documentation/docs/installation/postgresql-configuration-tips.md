<h1>PostgreSQL configuration tips</h1>

The default parameters of postgresql are usually very pessimistic about server capacity and set very low values.

This is simply a guide suggesting better values, but depends a lot about your server setup and configuration.

##RESOURCE USAGE section

  * **shared_buffers**: If you have a server with 1GB or more of RAM, a reasonable starting value for shared_buffers is 1/4 of the memory in your server.
  * **temp_buffers**: NPI
  * **work_mem**: It is best to set this value between 2MB and 10MB. If some query needs more memory, the value can be changed by the client in each connection using the command `SET work_mem = '32MB'`.
  * **maintenance_work_mem**: It's safe to set this value significantly larger than work_mem.
  
##WRITE AHEAD LOG section
  * **checkpoint_timeout**: The recommended value is 15 min.
  * **max_wal_size**: NPI
  
##QUERY TUNING section
  * **effective_cache_size**: Setting the value to 1/2 of total memory would be a normal conservative setting, and 3/4 of memory is a more aggressive but still reasonable amount. 