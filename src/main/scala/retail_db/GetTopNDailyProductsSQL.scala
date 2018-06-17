package retail_db

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

/**
  * Created by itversity on 11/06/18.
  * Selection or Projection - standardize, cleanse, masking, encrypt/decrypt
  * Filter - row level filtering (where), column level filtering (select)
  * Aggregate (total, per group)
  * Join the datasets
  * Sorting (totals, per group)
  * Ranking (total, per group)
  */

object GetTopNDailyProductsSQL {
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

    val topN = args(2).toInt
    val inputBaseDir = envProps.getString("json.input.path")
    val orders = spark.read.json(inputBaseDir + "/orders")
    orders.createTempView("orders")

    val orderItems = spark.read.json(inputBaseDir + "/order_items")
    orderItems.createTempView("order_items")

    val products = spark.read.json(inputBaseDir + "/products")
    products.createTempView("products")

    spark.sql("select * from (select q.*, " +
      "rank() over (partition by order_date order by order_revenue desc) rnk from " +
      "(select order_date, " +
      "product_name, " +
      "sum(order_item_subtotal) order_revenue " +
      "from orders join order_items " +
      "on order_id = order_item_order_id " +
      "join products " +
      "on product_id = order_item_product_id " +
      "group by order_date, product_name) q) q1 " +
      "where rnk <= " + topN + " " +
      "order by order_date, order_revenue desc").
      write.
      json(envProps.getString("output.path") + args(1))
  }

}


