package com.kee

import com.kee.data.DataDescription.{convertClick, convertLoan, convertOrder, convertUser}
import com.kee.data._
import com.kee.utils.{FeatureUtils, HDFSUtils, SparkUtils}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.storage.StorageLevel

object Entrance {

    def generateData() = {

        import com.kee.utils.SparkUtils.sqlContext.implicits._

        // 5张表转成parquet
        DataDescription.convertUser()
        DataDescription.convertLoanSum()
        DataDescription.convertLoan()
        DataDescription.convertOrder()
        DataDescription.convertClick()

        // 五个parquet join成一个大表 AllUser
        DataDescription.joinData()

        // 大表经过数据处理，得到RawFeature
        val allUser = DataDescription.loadAllUser()
        val rawFeats = DataDescription.generateRawFeature(allUser)
        val featMonth = Seq[Int]()
        val labelMonth = Seq[Int]()
        val feats = rawFeats.rdd.map(e => FeatureUtils.fe(e, featMonth, labelMonth))
        HDFSUtils.deleteIfExist("./spark/fes")
        feats.map(_.toString).saveAsTextFile("./spark/fes")

    }

    def main(args: Array[String]): Unit = {

        import com.kee.utils.SparkUtils.sqlContext.implicits._

        generateData()

    }

    def train() = {
        import com.kee.utils.SparkUtils.sqlContext.implicits._

        val users = DataDescription.loadAllUser()
        val filteredNovBehavior = DataDescription.filterNovBehavior(users)
        val feats = DataDescription.fe(filteredNovBehavior)
        val (train, validate) = DataDescription.cross(feats)

        train.persist(StorageLevel.MEMORY_AND_DISK)
        validate.persist(StorageLevel.MEMORY_AND_DISK)

        val numIterations = 100
        val stepSize = 0.00000001
        val model = LinearRegressionWithSGD.train(train.rdd, numIterations, stepSize)

        val valuesAndPreds = validate.map { point =>
            val prediction = model.predict(point.features)
            (point.label, 0.0)
        }

        // 2.5560072814763966
        // 2.5344604721758155
        println(DataDescription.calRMSE(valuesAndPreds))
    }

}
