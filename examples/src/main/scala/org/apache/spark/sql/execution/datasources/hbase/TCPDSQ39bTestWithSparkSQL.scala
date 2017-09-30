package org.apache.spark.sql.execution.datasources.hbase.examples

import org.apache.spark.sql._

object TCPDSQ39bTestWithSparkSQL {
  def main(args: Array[String]){
    val spark = SparkSession.builder()
      .appName("TCPDS_Q39b_TestingWithSparkHive")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    //HBase tables sparksql_inventory, sparksql_item, sparksql_warehouse, sparksql_date_dim are used in the following queries
    //TCPDS Q39b
    val timeStart = System.currentTimeMillis()
    val ret = sql("WITH inv AS (SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, stdev, mean, " +
      "CASE mean WHEN 0 THEN NULL ELSE stdev / mean END cov " +
      "FROM (SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, stddev_samp(inv_quantity_on_hand) stdev, " +
      "avg(inv_quantity_on_hand) mean FROM sparksql_inventory, sparksql_item, sparksql_warehouse, sparksql_date_dim " +
      "WHERE inv_item_sk = i_item_sk AND inv_warehouse_sk = w_warehouse_sk AND inv_date_sk = d_date_sk AND d_year = 2001 " +
      "GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo " +
      "WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1) " +
      "SELECT inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, inv2.w_warehouse_sk, inv2.i_item_sk, " +
      "inv2.d_moy, inv2.mean, inv2.cov FROM inv inv1, inv inv2 WHERE inv1.i_item_sk = inv2.i_item_sk AND " +
      "inv1.w_warehouse_sk = inv2.w_warehouse_sk AND inv1.d_moy = 1 AND inv2.d_moy = 1 + 1 AND inv1.cov > 1.5 " +
      "ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov , inv2.d_moy, inv2.mean, inv2.cov")
    ret.show()
    ret.count()
    val timeEnd = System.currentTimeMillis()
    println(s"Execution Time of TCPDS Q39b: ${timeEnd - timeStart}")
  }
}
