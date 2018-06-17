package retail_db

import org.apache.spark.sql.SparkSession
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

object GetDailyProductRevenue {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      master("local").
      appName("Get Daily Product Revenue").
      getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val inputBaseDir = "/Users/itversity/Research/data/retail_db_json"
    val orders = spark.read.json(inputBaseDir + "/orders").
      filter(col("order_status").isin("COMPLETE", "CLOSED"))

    val orderItems = spark.read.json(inputBaseDir + "/order_items")

    val orderItemRevenue = orders.
      join(orderItems, $"order_id" === $"order_item_order_id").
      select(orders("order_date"),
        orderItems("order_item_product_id"),
        orderItems("order_item_subtotal"))
    val dailyProductIdRevenue = orderItemRevenue.groupBy("order_date", "order_item_product_id").
      agg(round(sum("order_item_subtotal"), 2).alias("order_revenue"))

    val products = spark.read.json(inputBaseDir + "/products")
    val dailyProductRevenue = dailyProductIdRevenue.
      join(products, $"order_item_product_id" === $"product_id").
      select("order_date", "product_name", "order_revenue")
    dailyProductRevenue.orderBy($"order_date", $"order_revenue".desc).
      write.
      json("/Users/itversity/Research/data/dailyProductRevenue")
  }

}
