package com.kee

import com.kee.data._
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.storage.StorageLevel

object Entrance {

    def statistics(): Unit = {
        val users = DataDescription.loadAllUser()
        val collect = users.filter(_.loanSum.loanSum > 0.0).collect()
        println(users.filter(_.loanSum.loanSum > 0.0).count())
    }

    def main(args: Array[String]): Unit = {
        statistics()
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
