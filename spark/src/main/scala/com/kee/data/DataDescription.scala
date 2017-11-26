package com.kee.data

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import com.kee.utils.{HDFSUtils, SparkUtils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @param uid
  * @param age
  * @param sex        {1,2}
  * @param activeDate 激活日期
  * @param limit      初始额度
  */
case class User(var id: String, uid: Long, age: Int, sex: Int, activeDate: Date, limit: Double, var clicks: Seq[Click], var orders: Seq[Order], var loans: Seq[Loan], var loanSum: LoanSum)

/**
  *
  * @param uid
  * @param clickTime
  * @param pid   点击页面Id
  * @param param 点击页面所带参数
  */
case class Click(uid: Long, clickTime: Timestamp, pid: Long, param: Long)

// 月度点击数，不分页面和param

/**
  *
  * @param uid
  * @param buyDate
  * @param price    单价
  * @param number   购买数量
  * @param cateId   品类Id
  * @param discount 优惠金额
  */
case class Order(uid: Long, buyDate: Date, price: Double, number: Int, cateId: Long, discount: Double)

// 月度总订单金额
// 月度，max(单价*数量-优惠, 0)
// 月度订单数

/**
  *
  * @param uid
  * @param loanTime
  * @param loanAmount 贷款总数
  * @param planNum    分期数
  */
case class Loan(uid: Long, loanTime: Timestamp, loanAmount: Double, planNum: Int)

// 月度总额
// 月度频次

case class LoanSum(uid: Long, loanSum: Double)

object DataDescription {

    import SparkUtils.sqlContext.implicits._

    val PREFIX = "/Users/roliy/jdd_data"
    val USER_PATH = s"$PREFIX/user"
    val CLICK_PATH = s"$PREFIX/click"
    val ORDER_PATH = s"$PREFIX/order"
    val LOAN_PATH = s"$PREFIX/loan"
    val LOAN_SUM_PATH = s"$PREFIX/loan_sum"
    val ALL_USER_PATH = s"$PREFIX/all_user"
    val JOIN_ALL_USER_PATH = s"$PREFIX/join_all_user"
    val NOV_FIRST = 1477929600000L // 2016-11-01

    val random: Int = (Math.random() * 1000).toInt

    def convertUser(): DataFrame = {
        val sourcePath = "/Users/roliy/jdd_data/t_user.csv"
        HDFSUtils.deleteIfExist(USER_PATH)
        val df = SparkUtils.sparkContext.textFile(sourcePath)
                .zipWithIndex()
                .filter(_._2 != 0)
                .map(_._1)
                .map { e =>
                    val slices = e.split(",")
                    val dateSlices = slices(3).split("-").map(_.toInt)
                    val date = new Date(dateSlices(0) - 1900, dateSlices(1) - 1, dateSlices(2))
                    User("", slices(0).toLong, slices(1).toInt, slices(2).toInt, date, slices(4).toDouble,
                        Seq(), Seq(), Seq(), null)
                }.toDF()
        df.write.parquet(USER_PATH)
        df
    }

    def convertClick(): DataFrame = {
        val sourcePath = "/Users/roliy/jdd_data/t_click.csv"
        HDFSUtils.deleteIfExist(CLICK_PATH)
        val df = SparkUtils.sparkContext.textFile(sourcePath)
                .zipWithIndex()
                .filter(_._2 != 0)
                .map(_._1)
                .mapPartitions { partition =>
                    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    partition.map { e =>
                        val slices = e.split(",")
                        val date = dateFormat.parse(slices(1))
                        val timestamp = new Timestamp(date.getTime)
                        Click(slices(0).toLong, timestamp, slices(2).toLong, slices(3).toLong)
                    }
                }
                .toDF
        df.write.parquet(CLICK_PATH)
        df
    }

    def convertOrder(): DataFrame = {
        val sourcePath = "/Users/roliy/jdd_data/t_order.csv"
        HDFSUtils.deleteIfExist(ORDER_PATH)
        val df = SparkUtils.sparkContext.textFile(sourcePath)
                .zipWithIndex()
                .filter(_._2 != 0)
                .map(_._1)
                .map { e =>
                    val slices = e.split(",")
                    val dateSlices = slices(1).split("-").map(_.toInt)
                    val date = new Date(dateSlices(0) - 1900, dateSlices(1) - 1, dateSlices(2))
                    val price = if (slices(2).nonEmpty) slices(2).toDouble else -1
                    Order(slices(0).toLong, date, price, slices(3).toInt, slices(4).toLong, slices(5).toDouble)
                }.toDF()
        df.write.parquet(ORDER_PATH)
        df
    }

    def convertLoan(): DataFrame = {
        val sourcePath = "/Users/roliy/jdd_data/t_loan.csv"
        HDFSUtils.deleteIfExist(LOAN_PATH)
        val df = SparkUtils.sparkContext.textFile(sourcePath)
                .zipWithIndex()
                .filter(_._2 != 0)
                .map(_._1)
                .mapPartitions { partition =>
                    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    partition.map { e =>
                        val slices = e.split(",")
                        val date = dateFormat.parse(slices(1))
                        val timestamp = new Timestamp(date.getTime)
                        Loan(slices(0).toLong, timestamp, slices(2).toDouble, slices(3).toInt)
                    }
                }.toDF()
        df.write.parquet(LOAN_PATH)
        df
    }

    def convertLoanSum(): DataFrame = {
        val sourcePath = "/Users/roliy/jdd_data/t_loan_sum.csv"
        HDFSUtils.deleteIfExist(LOAN_SUM_PATH)
        val df = SparkUtils.sparkContext.textFile(sourcePath)
                .zipWithIndex()
                .filter(_._2 != 0)
                .map(_._1)
                .map { e =>
                    val slices = e.split(",")
                    LoanSum(slices(0).toLong, slices(2).toDouble)
                }.toDF()
        df.write.parquet(LOAN_SUM_PATH)
        df
    }

    def loadUser(): Dataset[User] = SparkUtils.sqlContext.read.parquet(USER_PATH).as[User]

    def loadClick(): Dataset[Click] = SparkUtils.sqlContext.read.parquet(CLICK_PATH).as[Click]

    def loadOrder(): Dataset[Order] = SparkUtils.sqlContext.read.parquet(ORDER_PATH).as[Order]

    def loadLoan(): Dataset[Loan] = SparkUtils.sqlContext.read.parquet(LOAN_PATH).as[Loan]

    def loadLoanSum(): Dataset[LoanSum] = SparkUtils.sqlContext.read.parquet(LOAN_SUM_PATH).as[LoanSum]

    def loadAllUser(): Dataset[User] = SparkUtils.sqlContext.read.parquet(JOIN_ALL_USER_PATH).as[User]

    def fillClick(_users: RDD[(Long, User)], _clicks: RDD[(Long, Click)], month: Int): RDD[(Long, User)] = {
        val clicks = _clicks.filter(e => e._2.clickTime.getMonth < month)
                .map(e => (e._1, Array(e._2)))
                .reduceByKey((e1, e2) => {
                    e1 ++ e2
                })
        _users.leftOuterJoin(clicks)
                .map {
                    case (uid, (user, _clicksOps)) =>
                        val _click: Seq[Click] = _clicksOps match {
                            case Some(v) => v
                            case _ => user.clicks
                        }
                        user.clicks = _click
                        (uid, user)
                }
    }

    def fillOrder(_user: RDD[(Long, User)], _orders: RDD[(Long, Order)], month: Int): RDD[(Long, User)] = {
        val orders = _orders.filter(_._2.buyDate.getMonth < month)
                .map(e => (e._1, Array(e._2)))
                .reduceByKey((e1, e2) => {
                    e1 ++ e2
                })
        _user.leftOuterJoin(orders)
                .map {
                    case (uid, (user, _orderOps)) =>
                        val _order: Seq[Order] = _orderOps match {
                            case Some(v) => v
                            case _ => user.orders
                        }
                        user.orders = _order
                        (uid, user)
                }
    }

    def fillLoans(_user: RDD[(Long, User)], _loans: RDD[(Long, Loan)], month: Int): RDD[(Long, User)] = {
        val loans = _loans.filter(_._2.loanTime.getMonth < month)
                .map(e => (e._1, Array(e._2)))
                .reduceByKey((e1, e2) => {
                    e1 ++ e2
                })
        _user.leftOuterJoin(loans)
                .map {
                    case (uid, (user, _loanOps)) =>
                        val _loan: Seq[Loan] = _loanOps match {
                            case Some(v) => v
                            case _ => user.loans
                        }
                        user.loans = _loan
                        (uid, user)
                }
    }

    /**
      * 将month月的loan聚集起来，作为loanSum
      *
      * @param _user
      * @param _loans
      * @param month
      */
    def fillLoanSum(_user: RDD[(Long, User)], _loans: RDD[(Long, Loan)], month: Int): RDD[(Long, User)] = {
        val loanSum = _loans.filter(_._2.loanTime.getMonth == month)
                .map(e => (e._1, LoanSum(e._1, e._2.loanAmount)))
                .reduceByKey((e1, e2) => {
                    LoanSum(e1.uid, e1.loanSum + e2.loanSum)
                })
        _user.leftOuterJoin(loanSum)
                .map {
                    case (uid, (user, _loanSumOps)) =>
                        val _loanSum: LoanSum = _loanSumOps match {
                            case Some(v) => v
                            case _ => LoanSum(uid, 0.0)
                        }
                        user.loanSum = _loanSum
                        (uid, user)
                }
    }

    /**
      * 过滤掉month之后(包括month)的行为数据，并拿month整月的loan做成loanSum
      *
      * @param rawUser
      * @param rawClick
      * @param rawOrder
      * @param rawLoan
      * @param _month 1-base 9表示9月
      * @return
      */
    def joinData(rawUser: Dataset[User], rawClick: Dataset[Click], rawOrder: Dataset[Order], rawLoan: Dataset[Loan], _month: Int): RDD[User] = {
        val month = _month - 1
        val withClick = fillClick(rawUser.rdd.map(e => (e.uid, e)),
            rawClick.rdd.map(e => (e.uid, e)), month)
        val withOrder = fillOrder(withClick, rawOrder.rdd.map(e => (e.uid, e)), month)
        val withLoan = fillLoans(withOrder, rawLoan.rdd.map(e => (e.uid, e)), month)
        fillLoanSum(withLoan, rawLoan.rdd.map(e => (e.uid, e)), month)
                .map(_._2)
                .map { user =>
                    user.id = s"${user.uid}_${_month}_${random}"
                    user
                }
    }

    def joinData() = {

        val users = DataDescription.loadUser()

        val clicks = DataDescription.loadClick().map(e => (e.uid, Array(e)))
                .rdd
                .reduceByKey((e1, e2) => {
                    e1 ++ e2
                })

        val withClick = users.rdd.map(e => (e.uid, e))
                .leftOuterJoin(clicks)
                .map {
                    case (uid, (user, _clicksOps)) =>
                        val _click: Seq[Click] = _clicksOps match {
                            case Some(v) => v
                            case _ => user.clicks
                        }
                        user.clicks = _click
                        (uid, user)
                }

        val orders = DataDescription.loadOrder().map(e => (e.uid, Array(e)))
                .rdd
                .reduceByKey((e1, e2) => {
                    e1 ++ e2
                })

        val withOrder = withClick
                .leftOuterJoin(orders)
                .map {
                    case (uid, (user, _orderOps)) =>
                        val _order: Seq[Order] = _orderOps match {
                            case Some(v) => v
                            case _ => user.orders
                        }
                        user.orders = _order
                        (uid, user)
                }

        val loans = DataDescription.loadLoan().map(e => (e.uid, Array(e)))
                .rdd
                .reduceByKey((e1, e2) => {
                    e1 ++ e2
                })

        val withLoans = withOrder
                .leftOuterJoin(loans)
                .map {
                    case (uid, (user, _loanOps)) =>
                        val _loan: Seq[Loan] = _loanOps match {
                            case Some(v) => v
                            case _ => user.loans
                        }
                        user.loans = _loan
                        (uid, user)
                }

        val loanSum = DataDescription.loadLoanSum().map(e => (e.uid, e)).rdd

        val withLoanSum = withLoans.leftOuterJoin(loanSum)
                .map {
                    case (uid, (user, _loanSumOps)) =>
                        val _loanSum: LoanSum = _loanSumOps match {
                            case Some(v) => v
                            case _ => LoanSum(uid, 0.0)
                        }
                        user.loanSum = _loanSum
                        (uid, user)
                }

        HDFSUtils.deleteIfExist(JOIN_ALL_USER_PATH)
        withLoanSum.map(_._2).toDF().write.parquet(JOIN_ALL_USER_PATH)

    }

    /**
      * 过滤掉11月的行为数据，避免穿越。而且预测12月数据时，也没有12月的行为数据。
      *
      * @param users
      */
    def filterNovBehavior(users: Dataset[User]): Dataset[User] = {
        val cmpTimestamp = new Timestamp(NOV_FIRST)
        val cmpDate = new Date(NOV_FIRST)
        users.map { user =>
            user.clicks = user.clicks.filter(_.clickTime.before(cmpTimestamp))
            user.orders = user.orders.filter(_.buyDate.before(cmpDate))
            user.loans = user.loans.filter(_.loanTime.before(cmpTimestamp))
            user
        }
    }

    def fe(users: Dataset[User]): Dataset[LabeledPoint] = {
        users.map { user =>
            // val feats = Array(user.uid, user.age, user.sex, user.activeDate.getTime, user.limit)
            val feats = Array(user.age.toDouble, user.limit, user.sex,
                user.activeDate.getYear,
                user.activeDate.getMonth,
                user.activeDate.getDate
            )
            LabeledPoint(user.loanSum.loanSum, Vectors.dense(feats))
        }
    }

    def cross(sample: Dataset[LabeledPoint]): (Dataset[LabeledPoint], Dataset[LabeledPoint]) = {
        val marked = sample.map { item =>
            if (Math.random() > 0.1) {
                (1, item)
            } else {
                (0, item)
            }
        }
        val train = marked.filter(_._1 == 1).map(_._2)
        val validate = marked.filter(_._1 == 0).map(_._2)
        (train, validate)
    }

    def calRMSE(predict: Dataset[(Double, Double)]): Double = {
        predict.cache()
        val count = predict.count()
        Math.sqrt(predict.map { e =>
            (e._1 - e._2) * (e._1 - e._2)
        }.reduce(_ + _) / count)
    }

    def generateData(): Unit = {

        import com.kee.utils.SparkUtils.sqlContext.implicits._

        val rawUsers = convertUser().as[User].persist(StorageLevel.MEMORY_AND_DISK)
        val rawClicks = convertClick().as[Click].persist(StorageLevel.MEMORY_AND_DISK)
        val rawLoans = convertLoan().as[Loan].persist(StorageLevel.MEMORY_AND_DISK)
        val rawOrders = convertOrder().as[Order].persist(StorageLevel.MEMORY_AND_DISK)
        // convertLoanSum()

        val month9 = DataDescription.joinData(rawUsers, rawClicks, rawOrders, rawLoans, 9).persist(StorageLevel.MEMORY_AND_DISK)
        val month10 = DataDescription.joinData(rawUsers, rawClicks, rawOrders, rawLoans, 10).persist(StorageLevel.MEMORY_AND_DISK)
        val month11 = DataDescription.joinData(rawUsers, rawClicks, rawOrders, rawLoans, 11).persist(StorageLevel.MEMORY_AND_DISK)

        val path = s"${PREFIX}/all_user"
        HDFSUtils.deleteIfExist(path)
        month9.union(month10).union(month11).toDF.write.parquet(path)

    }

    // case class User(var id: String, uid: Long, age: Int, sex: Int, activeDate: Date, limit: Double, var clicks: Seq[Click], var orders: Seq[Order], var loans: Seq[Loan], var loanSum: LoanSum)

    def featureExtract(users: Dataset[User]): Dataset[String] = {

        users.map { user =>

            val res = ArrayBuffer[String]()

            // 用户基础信息
            res.append(user.uid.toString)
            res.append(user.age.toString)
            res.append(user.sex.toString)
            res.append(s"${user.activeDate.getYear + 1900}-${user.activeDate.getMonth + 1}")
            res.append(user.limit.toString)

            // click按月聚集
            val clickValueMap = user.clicks.groupBy(_.clickTime.getMonth).map {
                case (key, clicks) => (key + 1, clicks.size)
            }
            for (i <- 8 to 11) {

                clickValueMap.get(i) match {
                    case Some(v) => res.append(v.toString)
                    case _ => res.append("0")
                }
            }

            // order按月聚集
            val orderValueMap = user.orders.groupBy(_.buyDate.getMonth).map {
                case (key, orders) =>
                    val month = key + 1
                    val sum = orders.map(e => Math.max(e.price * e.number - e.discount, 0)).sum
                    (month, (orders.size, sum))
            }
            for (i <- 8 to 11) {
                orderValueMap.get(i) match {
                    case Some(v) =>
                        res.append(v._1.toString)
                        res.append(v._2.toString)
                    case _ =>
                        res.append("0")
                        res.append("0.0")
                }

            }

            // loan按月聚合
            val loanValueMap = user.loans.groupBy(_.loanTime.getMonth).map {
                case (key, loans) =>
                    val month = key + 1
                    val sum = loans.map(_.loanAmount).sum
                    (month, (loans.size, sum))
            }
            for (i <- 8 to 11) {
                loanValueMap.get(i) match {
                    case Some(v) =>
                        res.append(v._1.toString)
                        res.append(v._2.toString)
                    case _ =>
                        res.append("0")
                        res.append("0.0")
                }

            }

            res.append(user.loanSum.loanSum.toString)
            String.join(",", res: _*)
        }

    }

}
