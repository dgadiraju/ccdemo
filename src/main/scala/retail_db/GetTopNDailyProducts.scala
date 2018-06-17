package retail_db

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

/**
  * Created by itversity on 11/06/18.
  * Selection or Projection - standardize, cleanse, masking, encrypt/decrypt
  * Filter - row level filtering (where), column level filtering (select)
  * Aggregate (total, per group)
  * Join the datasets
  * Sorting (totals, per group)
  * Ranking (total, per group)
  */

object GetTopNDailyProducts {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
//    (("dev.exectuion.mode" -> "local"), ("dev.input.path", ""), ())
    val envProps = props.getConfig(args(0))
//    (("execution.mode", "local"))
    val spark = SparkSession.
      builder().
      master(envProps.getString("execution.mode")).
      appName("Get Daily Product Revenue").
      getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    val topN = 5
    val inputBaseDir = envProps.getString("json.input.path")
    val orders = spark.read.json(inputBaseDir + "/orders").
      filter(col("order_status").isin("COMPLETE", "CLOSED"))

    val orderItems = spark.read.json(inputBaseDir + "/order_items")

    val orderItemRevenue = orders.
      join(orderItems, $"order_id" === $"order_item_order_id").
      select(orders("order_date"),
        orderItems("order_item_product_id"),
        orderItems("order_item_subtotal"))

    val dailyTopNProductIds = orderItemRevenue.
      groupBy("order_date", "order_item_product_id").
      agg(round(sum("order_item_subtotal"), 2).alias("order_revenue")).
      withColumn("rnk", rank().
        over(Window.
          partitionBy("order_date").
          orderBy($"order_revenue".desc)
        )
      ).
      filter($"rnk" <= topN)

    val products = spark.read.json(inputBaseDir + "/products")
    val dailyProductRevenue = dailyTopNProductIds.
      join(products, $"order_item_product_id" === $"product_id").
      select("order_date", "product_name", "order_revenue", "rnk")
    dailyProductRevenue.orderBy($"order_date", $"order_revenue".desc).
      write.
      json(envProps.getString("output.path") + args(1))
  }

}

