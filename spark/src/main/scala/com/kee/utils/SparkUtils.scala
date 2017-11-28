package com.kee.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {

    lazy val sparkConf: SparkConf = buildSparkConf()

    lazy val sparkContext: SparkContext = buildSparkContext()

    lazy val sqlContext: SQLContext = buildSQLContext()

    private
    def buildSparkConf() = {
        val sparkConf = new SparkConf()
        sparkConf.setAppName("SparkDomain")
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[4]")
        }
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.kryoserializer.buffer.max", "512m")
        sparkConf
    }

    private
    def buildSparkContext() = {
        val res = SparkContext.getOrCreate(sparkConf)
        res
    }

    private
    def buildSQLContext() = {
        val sqlContext = SQLContext.getOrCreate(sparkContext)
        sqlContext
    }

}
