/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, DataFrame}

case class PRecord1(col00: String,
                   col01: Int,
                   col1: Boolean,
                   col2: Double,
                   col3: Float,
                   col4: Int,
                   col5: Long,
                   col6: Short,
                   col7: String,
                   col8: Byte)

object PRecord1 {
  def apply(i: Int): PRecord1 = {
    PRecord1(s"row${"%03d".format(i)}",
      if (i % 2 == 0) {
        i
      } else {
        -i
      },
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}

object Phoenix1 {
  def cat = s"""{
                |"table":{"namespace":"default", "name":"SHC", "tableCoder":"Phoenix"},
                |"rowkey":"key1:key2",
                |"columns":{
                |"col00":{"cf":"rowkey", "col":"key1", "type":"string"},
                |"col01":{"cf":"rowkey", "col":"key2", "type":"int"},
                |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
                |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
                |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
                |}
                |}""".stripMargin

  def main(args: Array[String]){
    val sparkConf = new SparkConf().setAppName("CompositeKeyTest1")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    //populate table with composite key
    val data = (30 to 31).map { i =>
      PRecord1(i)
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    //full query
    val df = withCatalog(cat)
    df.show
    sc.stop()
  }
}
