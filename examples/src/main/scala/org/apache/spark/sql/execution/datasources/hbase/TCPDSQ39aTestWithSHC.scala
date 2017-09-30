package org.apache.spark.sql.execution.datasources.hbase.examples

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog


object TCPDSQ39aTestWithSHC {
  // catalog for the HBase table named 'shcHbase_item'
  val item_cat = s"""{
                |"table":{"namespace":"default", "name":"shcHbase_item", "tableCoder":"PrimitiveType"},
                |"rowkey":"key",
                |"columns":{
                |"i_item_sk":{"cf":"rowkey", "col":"key", "type":"int"},
                |"i_item_id":{"cf":"a", "col":"i_item_id", "type":"string"},
                |"i_rec_start_date":{"cf":"b", "col":"i_rec_start_date", "type":"string"},
                |"i_rec_end_date":{"cf":"b", "col":"i_rec_end_date", "type":"string"},
                |"i_item_desc":{"cf":"c", "col":"i_item_desc", "type":"string"},
                |"i_current_price":{"cf":"c", "col":"i_current_price", "type":"double"},
                |"i_wholesale_cost":{"cf":"c", "col":"i_wholesale_cost", "type":"double"},
                |"i_brand_id":{"cf":"d", "col":"i_brand_id", "type":"int"},
                |"i_brand":{"cf":"d", "col":"i_brand", "type":"string"},
                |"i_class_id":{"cf":"d", "col":"i_class_id", "type":"int"},
                |"i_class":{"cf":"d", "col":"i_class", "type":"string"},
                |"i_category_id":{"cf":"d", "col":"i_category_id", "type":"int"},
                |"i_category":{"cf":"d", "col":"i_category", "type":"string"},
                |"i_manufact_id":{"cf":"d", "col":"i_manufact_id", "type":"int"},
                |"i_manufact":{"cf":"d", "col":"i_manufact", "type":"string"},
                |"i_size":{"cf":"e", "col":"i_size", "type":"string"},
                |"i_formulation":{"cf":"e", "col":"i_formulation", "type":"string"},
                |"i_color":{"cf":"e", "col":"i_color", "type":"string"},
                |"i_units":{"cf":"e", "col":"i_units", "type":"string"},
                |"i_container":{"cf":"e", "col":"i_container", "type":"string"},
                |"i_manager_id":{"cf":"f", "col":"i_manager_id", "type":"int"},
                |"i_product_name":{"cf":"f", "col":"i_product_name", "type":"string"}
                |}
                |}""".stripMargin

  // catalog for the HBase table named 'shcHbase_date_dim'
  val date_dim_cat = s"""{
                 |"table":{"namespace":"default", "name":"shcHbase_date_dim", "tableCoder":"PrimitiveType"},
                 |"rowkey":"key",
                 |"columns":{
                 |"d_date_sk":{"cf":"rowkey", "col":"key", "type":"int"},
                 |"d_date_id":{"cf":"a", "col":"d_date_id", "type":"string"},
                 |"d_date":{"cf":"a", "col":"d_date", "type":"string"},
                 |"d_month_seq":{"cf":"a", "col":"d_month_seq", "type":"int"},
                 |"d_week_seq":{"cf":"a", "col":"d_week_seq", "type":"int"},
                 |"d_quarter_seq":{"cf":"a", "col":"d_quarter_seq", "type":"int"},
                 |"d_year":{"cf":"a", "col":"d_year", "type":"int"},
                 |"d_dow":{"cf":"b", "col":"d_dow", "type":"int"},
                 |"d_moy":{"cf":"b", "col":"d_moy", "type":"int"},
                 |"d_dom":{"cf":"b", "col":"d_dom", "type":"int"},
                 |"d_qoy":{"cf":"b", "col":"d_qoy", "type":"int"},
                 |"d_fy_year":{"cf":"c", "col":"d_fy_year", "type":"int"},
                 |"d_fy_quarter_seq":{"cf":"c", "col":"d_fy_quarter_seq", "type":"int"},
                 |"d_fy_week_seq":{"cf":"c", "col":"d_fy_week_seq", "type":"int"},
                 |"d_day_name":{"cf":"d", "col":"d_day_name", "type":"string"},
                 |"d_quarter_name":{"cf":"d", "col":"d_quarter_name", "type":"string"},
                 |"d_holiday":{"cf":"d", "col":"d_holiday", "type":"string"},
                 |"d_weekend":{"cf":"d", "col":"d_weekend", "type":"string"},
                 |"d_following_holiday":{"cf":"d", "col":"d_following_holiday", "type":"string"},
                 |"d_first_dom":{"cf":"e", "col":"d_first_dom", "type":"int"},
                 |"d_last_dom":{"cf":"e", "col":"d_last_dom", "type":"int"},
                 |"d_same_day_ly":{"cf":"e", "col":"d_same_day_1y", "type":"int"},
                 |"d_same_day_lq":{"cf":"e", "col":"d_same_day_1q", "type":"int"},
                 |"d_current_day":{"cf":"f", "col":"d_current_day", "type":"string"},
                 |"d_current_week":{"cf":"f", "col":"d_current_week", "type":"string"},
                 |"d_current_month":{"cf":"f", "col":"d_current_month", "type":"string"},
                 |"d_current_quarter":{"cf":"f", "col":"d_current_quarter", "type":"string"},
                 |"d_current_year":{"cf":"f", "col":"d_current_year", "type":"string"}
                 |}
                 |}""".stripMargin

  // catalog for the HBase table named 'shcHbase_warehouse'
  val warehouse_cat = s"""{
                |"table":{"namespace":"default", "name":"shcHbase_warehouse", "tableCoder":"PrimitiveType"},
                |"rowkey":"key",
                |"columns":{
                |"w_warehouse_sk":{"cf":"rowkey", "col":"key", "type":"int"},
                |"w_warehouse_id":{"cf":"a", "col":"w_warehouse_id", "type":"string"},
                |"w_warehouse_name":{"cf":"a", "col":"w_warehouse_name", "type":"string"},
                |"w_warehouse_sq_ft":{"cf":"a", "col":"w_warehouse_sq_ft", "type":"int"},
                |"w_street_number":{"cf":"b", "col":"w_street_number", "type":"string"},
                |"w_street_name":{"cf":"b", "col":"w_street_name", "type":"string"},
                |"w_street_type":{"cf":"b", "col":"w_street_type", "type":"string"},
                |"w_suite_number":{"cf":"b", "col":"w_suite_number", "type":"string"},
                |"w_city":{"cf":"c", "col":"w_city", "type":"string"},
                |"w_county":{"cf":"c", "col":"w_county", "type":"string"},
                |"w_state":{"cf":"c", "col":"w_state", "type":"string"},
                |"w_zip":{"cf":"c", "col":"w_zip", "type":"string"},
                |"w_country":{"cf":"c", "col":"w_country", "type":"string"},
                |"w_gmt_offset":{"cf":"d", "col":"w_gmt_offset", "type":"double"}
                |}
                |}""".stripMargin

  // catalog for the HBase table named 'shcHbase_inventory'
  val inventory_cat = s"""{
                 |"table":{"namespace":"default", "name":"shcHbase_inventory", "tableCoder":"PrimitiveType"},
                 |"rowkey":"key",
                 |"columns":{
                 |"inv_date_sk":{"cf":"rowkey", "col":"key", "type":"int"},
                 |"inv_item_sk":{"cf":"a", "col":"inv_item_sk", "type":"int"},
                 |"inv_warehouse_sk":{"cf":"a", "col":"inv_warehouse_sk", "type":"int"},
                 |"inv_quantity_on_hand":{"cf":"b", "col":"inv_quantity_on_hand", "type":"int"}
                 |}
                 |}""".stripMargin

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("TCPDS_Q39a_TestingWithSHC")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    import spark.sql

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    def saveToHBase(dataFrame: DataFrame, cat: String) = {
      dataFrame.write.options(
        Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }

    // load data from Hive tables into Hbase tables
    /*val itemHive = sql("SELECT * FROM item")
      .withColumn("i_current_price", $"i_current_price".cast(DoubleType))
      .withColumn("i_wholesale_cost", $"i_wholesale_cost".cast(DoubleType))
    saveToHBase(itemHive, item_cat)

    val date_dimHive = sql("SELECT * FROM date_dim")
    saveToHBase(date_dimHive, date_dim_cat)

    val warehouseHive = sql("SELECT * FROM warehouse")
      .withColumn("w_gmt_offset", $"w_gmt_offset".cast(DoubleType))
    saveToHBase(warehouseHive, warehouse_cat)

    val inventoryHive = sql("SELECT * FROM inventory")
    saveToHBase(inventoryHive, inventory_cat)*/

    // read data from hbase tables(hbase_item, hbase_date_dim, hbase_warehouse, hb_inventory) into dataframes
    val item_df = withCatalog(item_cat)
    item_df.createOrReplaceTempView("hbase_item")

    val date_dim_df = withCatalog(date_dim_cat)
    date_dim_df.createOrReplaceTempView("hbase_date_dim")

    val warehouse_df = withCatalog(warehouse_cat)
    warehouse_df.createOrReplaceTempView("hbase_warehouse")

    val inventory_df = withCatalog(inventory_cat)
    inventory_df.createOrReplaceTempView("hbase_inventory")

    //TCPDS Q39a
    val timeStart = System.currentTimeMillis()
    val ret = sqlContext.sql("WITH inv AS (SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, stdev, mean, " +
      "CASE mean WHEN 0 THEN NULL ELSE stdev / mean END cov FROM " +
      "(SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, stddev_samp(inv_quantity_on_hand) stdev, " +
      "avg(inv_quantity_on_hand) mean FROM hbase_inventory, hbase_item, hbase_warehouse, hbase_date_dim " +
      "WHERE inv_item_sk = i_item_sk AND inv_warehouse_sk = w_warehouse_sk AND inv_date_sk = d_date_sk AND d_year = 2001 " +
      "GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1) " +
      "SELECT inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, inv2.w_warehouse_sk, inv2.i_item_sk, " +
      "inv2.d_moy, inv2.mean, inv2.cov FROM inv inv1, inv inv2 WHERE inv1.i_item_sk = inv2.i_item_sk " +
      "AND inv1.w_warehouse_sk = inv2.w_warehouse_sk AND inv1.d_moy = 1 AND inv2.d_moy = 1 + 1 " +
      "ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov , inv2.d_moy, inv2.mean, inv2.cov")
    ret.show()
    ret.count()
    val timeEnd = System.currentTimeMillis()
    println(s"Execution Time of TCPDS Q39a: ${timeEnd - timeStart}")
  }
}
