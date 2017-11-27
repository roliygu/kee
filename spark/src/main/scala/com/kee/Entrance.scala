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
        val path = "/Users/roliy/jdd_data/raw_feature"
        HDFSUtils.deleteIfExist(path)
        DataDescription.generateRawFeature(allUser).toDF().write.parquet(path)

    }

    def main(args: Array[String]): Unit = {

        import com.kee.utils.SparkUtils.sqlContext.implicits._

        val rawFeature = DataDescription.loadRawFeature().rdd

        val month8Path = "./spark/month8"
        HDFSUtils.deleteIfExist(month8Path)
        rawFeature.map(e => FeatureUtils.feByMonth(e, 8, true))
                .map(_.toString).repartition(1).saveAsTextFile(month8Path)

        val month9Path = "./spark/month9"
        HDFSUtils.deleteIfExist(month9Path)
        rawFeature.map(e => FeatureUtils.feByMonth(e, 9, true))
                .map(_.toString).repartition(1).saveAsTextFile(month9Path)

        val month10Path = "./spark/month10"
        HDFSUtils.deleteIfExist(month10Path)
        rawFeature.map(e => FeatureUtils.feByMonth(e, 10, true))
                .map(_.toString).repartition(1).saveAsTextFile(month10Path)

        val month11Path = "./spark/month11"
        HDFSUtils.deleteIfExist(month11Path)
        rawFeature.map(e => FeatureUtils.feByMonth(e, 11, false))
                .map(_.toString).repartition(1).saveAsTextFile(month11Path)


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
