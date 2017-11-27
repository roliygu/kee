package com.kee.utils

import com.kee.data.RawFeature

import scala.collection.mutable.ArrayBuffer

class Feature {

}

case class Discrete(slot: String, value: String) extends Feature {
    override def toString: String = {
        val index = slot.concat(value).hashCode % FeatureUtils.round
        s"$index:1"
    }
}

case class Continuous(slot: String, value: Double) extends Feature {
    override def toString: String = {
        val index = slot.hashCode % FeatureUtils.round
        s"$index:$value"
    }

}

case class Instance(var _label: String = "0.0", features: ArrayBuffer[Feature] = ArrayBuffer[Feature]()) {

    def labeled(label: Any): Instance = {
        _label = label.toString
        this
    }

    def addDiscrete(slot: String, value: Any): Instance = {
        features.append(Discrete(slot, value.toString))
        this
    }

    def addContinuous(slot: String, value: Double): Instance = {
        features.append(Continuous(slot, value))
        this
    }

    override def toString: String = {
        val sb = new StringBuilder()
        sb.append(_label)
        for(feat <- features){
            sb.append(" ")
            sb.append(feat.toString)
        }
        sb.toString()
    }
}


object FeatureUtils {

    val round = 1000

    def fe(item: RawFeature, featMonth: Seq[Int], labelMonth: Seq[Int]): Instance = {
        Instance().labeled(item.loanSum.loanSum)
                .addDiscrete("age", item.age)
                .addDiscrete("sex", item.sex)
                .addContinuous("limit", item.limit)
    }


}
