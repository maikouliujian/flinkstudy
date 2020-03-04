package asyncio

import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import com.stumbleupon.async.Callback
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.hbase.async.{GetRequest, HBaseClient, KeyValue}

class HbaseAsyncLRU (zk: String, tableName: String, maxSize: Long, ttl: Long) extends RichAsyncFunction[String, String] {

  private var hbaseClient: HBaseClient = _
  private var cache: Cache[String, String] = _

  override def open(parameters: Configuration): Unit = {

    //guava Cache
    hbaseClient = new HBaseClient(zk)
    cache = CacheBuilder.newBuilder()
      .maximumSize(maxSize)
      .expireAfterWrite(ttl, TimeUnit.SECONDS)
      .build()

  }


  override def asyncInvoke(input: String, resultFuture:ResultFuture[String]): Unit = {

    val key = parseKey(input)
    val value = cache.getIfPresent(key)
    if (value != null) {
      val newV: String = fillData(input, value)
      import scala.collection.JavaConverters._
      resultFuture.complete(Collections.singleton(newV).asScala)
      return

    }

    val get = new GetRequest(tableName, key)

    hbaseClient.get(get).addCallbacks(new Callback[String, util.ArrayList[KeyValue]] {
      override def call(t: util.ArrayList[KeyValue]): String = {
        val v = parseRs(t)
        cache.put(key, v)
        import scala.collection.JavaConverters._
        resultFuture.complete(Collections.singleton(v).asScala)
        ""

      }

    }, new Callback[String, Exception] {
      override def call(t: Exception): String = {
        t.printStackTrace()
        resultFuture.complete(null)
        ""
      }
    })

  }

  private def parseKey(input: String): String = {
    ""
  }

  private def fillData(input: String, value: String): String = {
    ""
  }

  private def parseRs(t: util.ArrayList[KeyValue]): String = {
    ""
  }

}
