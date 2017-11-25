package com.kee

import com.kee.data._

object Entrance {

    def main(args: Array[String]): Unit = {

        val users = DataDescription.loadAllUser().cache()
        val count = users.count()

        val first = users.filter(_.loanSum.loanSum > 0.0).first()

        println("a")

    }

}
