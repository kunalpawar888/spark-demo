package org.shopdemo

import org.apache.spark.{SparkConf, SparkContext}

object customer {

  def main(args : Array[String]): Unit =
  {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Word Count")
    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(line ⇒ line.split(" ")).map(word ⇒ (word, 1)).reduceByKey(_ + _)
      .saveAsTextFile(args(1))
  }

}
