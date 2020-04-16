package com.qinglianyun.common

/**
  * @ Author ：liuhao
  * @ Company: qinglian cloud
  * @ Date   ：Created in 2020/4/2 15:22 
  * @ 
  */
object ESConsts {
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
