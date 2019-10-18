package com.qinglianyun.common

/**
  * @ Author ：liuhao
  * @ Company: qinglianyun
  * @ Date   ：Created in 11:17 2019/3/6
  * @ DESC： 可以在SparkConf中设置的一些配置，仅列举了需要的几条
  */
object SparkConfConfig {
  val SPARK_SERIALIZER = "spark.serializer"

  val SPARK_RDD_COMPRESS = "spark.rdd.compress"

  val SPARK_CONF_SET_MAX_SPOUT_PENDING = "conf.setMaxSpoutPending"

  val SPARK_STREAMING_BACKPRESSURE_ENABLED = "spark.streaming.backpressure.enabled"

  val SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION = "spark.streaming.kafka.maxRatePerPartition"

  val SPARK_SHUFFLE_FILE_BUFFER = "spark.shuffle.file.buffer" //默认为32k

  val SPARK_REDUCER_MAX_SIZE_IN_FLIGHT = "spark.reducer.maxSizeInFlight" // 默认为48M

  val SPARK_EXECUTOR_HEARTBEAT_INTERVAL = "spark.executor.heartbeatInterval"

  // Spark Sql
  val SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions" // 默认200

  val SPARK_SQL_BROADCAST_TIMEOUT = "spark.sql.broadcastTimeout"

  // RDD 分区
  val SPARK_DEFAULT_PARALLELISM = "spark.default.parallelism" // 默认 core数

  // shuffle时使用的传输协议
  val SPARK_SHUFFLE_BLOCK_TRANSFER_SERVICE = "spark.shuffle.blockTransferService" // 默认是netty，可以改为nio

  //解决网络超时问题
  val SPARK_CORE_CONNECTION_ACK_WAIT_TIMEOUT = "spark.core.connection.ack.wait.timeout" // 改成5分钟或者更高是（根据具体情况而定）
  val SPARK_AKKA_TIMEOUT = "spark.akka.timeout"
  val SPARK_STORAGE_BLOCK_MANAGER_SLAVE_TIMEOUT_MS = "spark.storage.blockManagerSlaveTimeoutMs"
  val SPARK_SHUFFLE_IO_CONNECTION_TIMEOUT = "spark.shuffle.io.connectionTimeout"
  val SPARK_RPC_ASK_TIMEOUT = "spark.rpc.askTimeout" //  or spark.rpc.lookupTimeout

  // SparkStreaming
  val SPARK_AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms"

  val SPARK_STREAMING_STOP_GRACEFULLY_ON_SHUTDOWN = "spark.streaming.stopGracefullyOnShutdown"

  // es support spark - connect
  val ES_INDEX_AUTO_CREATE = "es.index.auto.create"

  val ES_NODES = "es.nodes" // 默认localhost

  val ES_PORT = "es.port" // 默认9200

  val ES_NODES_PATH_PREFIX = "es.nodes.path.prefix" // 默认为空

  val ES_MAPPING_ID = "es.mapping.id"

  // es support spark - source
  val ES_RESOURCE = "es.resource" // 默认写入es的index和type 格式： index/type 目前不支持type

  val ES_RESOURCE_READ = "es.resource.read" // 默认是es.source的值

  // 对于多source可以写成: es.resource.write = my-collection/{media_type} 或者 格式化： my-collection/{@timestamp|yyyy.MM.dd}
  val ES_RESOURCE_WRITE = "es.resource.write" // 默认是es.source

  val ES_QUERY = "es.query" // 查询语句

  val ES_MAPPING_INCLUDE = "es.mapping.include" // 返回结果中包含的字段/属性

  val ES_MAPPING_EXCLUDE = "es.mapping.exclude" // 返回的结果中不包含的值

  val ES_BATCH_WRITE_RETRY_COUNT = "es.batch.write.retry.count" // 默认为3

  val ES_BATCH_WRITE_RETRY_WAIT = "es.batch.write.retry.wait" // 默认为10

  // es support spark - spark sql
  val ES_SPARK_DATAFRAME_WRITE_NULL = "es.spark.dataframe.write.null" // 默认是忽略null值，但是DataFrame是结构化数据，null值不能省略，将此配置设置为true
}
