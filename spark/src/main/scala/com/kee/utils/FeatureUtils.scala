package com.kee.utils

import com.kee.data.RawFeature

import scala.collection.mutable.ArrayBuffer

class Feature {

}

case class Discrete(slot: String, value: String) extends Feature {
    override def toString: String = {
        val index = Math.abs(slot.concat(value).hashCode) % FeatureUtils.round
        s"$index:1"
    }
}

case class Continuous(slot: String, value: Double) extends Feature {
    override def toString: String = {
        val index = Math.abs(slot.hashCode) % FeatureUtils.round
        s"$index:$value"
    }

}

case class Instance(var _label: String = "0", features: ArrayBuffer[Feature] = ArrayBuffer[Feature]()) {

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

    def feByMonth(item: RawFeature, month: Int, label: Boolean): Instance = {
        val index = month - 7
        val instance = Instance().addDiscrete("age", item.age)
                .addDiscrete("sex", item.sex)
                .addContinuous("limit", item.limit)
                .addContinuous(s"clickNum_${index}", item.clickNum(index - 1))
                .addContinuous(s"orderNum_${index}", item.orderNum(index - 1))
                .addContinuous(s"orderAmount_${index}", item.orderAmount(index - 1))
                .addContinuous(s"loanNum_${index}", item.loanNum(index - 1))
                .addContinuous(s"loanAmount_${index}", item.loanAmount(index - 1))

        if(label){
            instance.labeled(item.loanAmount(index))
        }

        instance
    }

    def feStep2Nov(item: RawFeature): Instance = {
        val index = 2
        Instance().addDiscrete("age", item.age)
                .addDiscrete("sex", item.sex)
                .addContinuous("limit", item.limit)
                .addContinuous(s"clickNum_${index}", item.clickNum(index - 1))
                .addContinuous(s"orderNum_${index}", item.orderNum(index - 1))
                .addContinuous(s"orderAmount_${index}", item.orderAmount(index - 1))
                .addContinuous(s"loanNum_${index}", item.loanNum(index - 1))
                .addContinuous(s"loanAmount_${index}", item.loanAmount(index - 1))
                .labeled(item.loanSum.loanSum)
    }

    def feStep2Dec(item: RawFeature): Instance = {
        val index = 3
        Instance().addDiscrete("age", item.age)
                .addDiscrete("sex", item.sex)
                .addContinuous("limit", item.limit)
                .addContinuous(s"clickNum_${index}", item.clickNum(index - 1))
                .addContinuous(s"orderNum_${index}", item.orderNum(index - 1))
                .addContinuous(s"orderAmount_${index}", item.orderAmount(index - 1))
                .addContinuous(s"loanNum_${index}", item.loanNum(index - 1))
                .addContinuous(s"loanAmount_${index}", item.loanAmount(index - 1))
    }


}
