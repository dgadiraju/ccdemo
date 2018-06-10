package retail_db

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by itversity on 04/06/18.
  */
object GetRevenueForEachOrder {
  def main(args: Array[String]): Unit = {
    /*
     * Create configuration object
     * Create Spark Context using configuration object
     * Read data using APIs under Spark Context
     * Process data
     * Write data using APIs under RDD
     *
     */

    val env = args(0)
    val props = ConfigFactory.load
    val conf = new SparkConf().
      setMaster(props.getConfig(env).getString("execution.mode")).
      setAppName("Get Revenue For Each Order")
    val sc = new SparkContext(conf)
    val orderItems: RDD[String] = sc.
      textFile(props.getConfig(env).getString("input.path") + "/order_items")

    orderItems.
      map(s => (s.split(",")(1).toInt, s.split(",")(4).toFloat)).
      reduceByKey((total, value) => total + value).
      map(t => t._1 + "," + t._2).
      saveAsTextFile(props.getConfig(env).getString("output.path") + "/ccdemo")

  }

}
