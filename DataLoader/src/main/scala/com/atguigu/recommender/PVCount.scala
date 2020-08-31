package com.atguigu.recommender

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Copyright (C), 1996-2020, 
  * FileName: PVCount
  * Author:   yankun
  * Date:     2020/7/29 21:40
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */
object PVCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PVCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val logRDD = sc.textFile("")
    logRDD.flatMap(_.split("|")).map(
      dataList => {
        val list = dataList.indexOf("\"")
        val list2 = dataList.lastIndexOf("\"")
        dataList.substring(list,list2)

      }
    )
  }

}
