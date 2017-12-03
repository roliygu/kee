package com.kee.increment

import java.io.FileWriter

import com.kee.utils.SparkUtils
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object IncrementProcessor {

  val df = SparkUtils.sparkContext.textFile("file:///Users/huangyajian/Desktop/jd/processed/part-00000")

  val schema = Source.fromFile("/Users/huangyajian/Desktop/jd/processed/schema").getLines.toList


  def main(args: Array[String]): Unit = {

    var indexList = List[Array[Int]](Array[Int](5, 6, 7), Array[Int](21, 22, 23), Array[Int](25, 26, 27), Array[Int](29, 30, 31), Array[Int](33, 34, 35),
      Array[Int](41, 42, 43), Array[Int](45, 46, 47), Array[Int](49, 50, 51))


    val doubleDf = incrementDouble(indexList, df)

    indexList = List[Array[Int]](Array[Int](13, 14, 15), Array[Int](9, 10, 11), Array[Int](17, 18, 19), Array[Int](37, 38, 39))

    val resData = incrementMap(indexList, doubleDf)

    resData.repartition(1).saveAsTextFile("file:///Users/huangyajian/Desktop/jd/processed/increment")

  }

  def addDoubleCol(addCol: RDD[Double], df: RDD[String]): RDD[String] = {
    val resMap = addCol.zipWithIndex().map(a => (a._2, a._1.toString))
    val dfMap = df.zipWithIndex().map(a => (a._2, a._1))
    val joinMap = dfMap.join(resMap).sortByKey()
    val res = joinMap.map(a => a._2._1 + "," + a._2._2)
    res
  }

  def addDoubleValue(rdd1: RDD[String], rdd2: RDD[Double]): RDD[Double] = {
    val rdd1Map = rdd1.zipWithIndex().map(a => (a._2, a._1.toDouble))
    val rdd2Map = rdd2.zipWithIndex().map(a => (a._2, a._1))
    val sum = rdd1Map.union(rdd2Map).reduceByKey((x, y) => x + y).sortByKey()
    val res = sum.values
    res
  }

  def incrementDouble(arrayList: List[Array[Int]], doubleDf: RDD[String]): RDD[String] = {
    var resData = doubleDf
    for (array <- arrayList) {
      val reverseArray = array.reverse
      val rdd2Array = new ArrayBuffer[Double]
      for (i <- 1 to df.count().toInt) {
        rdd2Array += 0.0f
      }
      var rdd2 = SparkUtils.sparkContext.makeRDD(rdd2Array.toArray[Double])
      for (index <- 1 to reverseArray.length) {
        val rdd1 = df.map(line => line.split(",")(reverseArray(index - 1)))
        val res = addDoubleValue(rdd1, rdd2)
        addSchema(index, getColName(reverseArray(index - 1)))
        resData = addDoubleCol(res, resData)
        rdd2 = res
      }
    }
    resData
  }


  def addTupleCol(addCol: RDD[(Long, String)], df: RDD[String]): RDD[String] = {
    val dfMap = df.zipWithIndex().map(a => (a._2, a._1))
    val joinMap = dfMap.join(addCol).sortByKey()
    val res = joinMap.map(a => a._2._1 + "," + a._2._2)
    res
  }

  def addTuple(rdd1: RDD[(Long, Array[(String, String)])], rdd2: RDD[(Long, Array[(String, String)])]): RDD[(Long, String)] = {
    val rdd3 = rdd1.join(rdd2).sortByKey()
    val rdd4 = rdd3.mapValues {
      values => (values._1.map(a => (a._1, a._2.toDouble)).toMap, values._2.map(a => (a._1, a._2.toDouble)).toMap)
    }.mapValues {
      values =>
        (values._1 /: values._2) {
          case (map, (k, v)) =>
            map + (k -> (v + map.getOrElse(k, 0.0d)))
        }
    }.mapValues {
      values =>
        values.map {
          case (k, v) => s"${k}:${v}"
        }.mkString(";")
    }
    rdd4
  }

  def incrementMap(arrayList: List[Array[Int]], doubleDf: RDD[String]): RDD[String] = {
    var resData = doubleDf
    for (array <- arrayList) {
      val reverseArray = array.reverse

      val res = df.map(
        line => line.split(",")(reverseArray(0))).zipWithIndex.map {
        pair => (pair._2, pair._1)
      }
      addSchema(1, getColName(reverseArray(0)))
      resData = addTupleCol(res, resData)

      var sum = res;

      for (index <- 1 until reverseArray.length) {
        val pre = sum.mapValues {
          sentence =>
            sentence.split(";").flatMap {
              tup =>
                if (tup.split(":").length != 2)
                  None
                else
                  Option((tup.split(":")(0), tup.split(":")(1)))
            }
        }
        println("index:" + index + ",colIndex:" + reverseArray(index))
        val rdd2 = df.map(line => line.split(",")(reverseArray(index))).zipWithIndex.map {
          pair => (pair._2, pair._1)
        }.mapValues {
          sentence =>
            sentence.split(";").flatMap {
              tup =>
                if (tup.split(":").length != 2)
                  None
                else
                  Option((tup.split(":")(0), tup.split(":")(1)))
            }
        }
        sum = addTuple(pre, rdd2)
        addSchema(index + 1, getColName(reverseArray(index)))
        resData = addTupleCol(sum, resData)
      }
    }
    resData
  }


  def addSchema(index: Int, colName: String): Unit = {
    val writer = new FileWriter("/Users/huangyajian/Desktop/jd/processed/add_schema", true)
    writer.write("\n" + colName + "Latest" + index + "Month")
    writer.close()
  }

  def getColName(index: Int): String = {
    schema(index).split(" ")(0)
  }

}
