package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

class Sapient {

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Sapient question").enableHiveSupport().getOrCreate()

		import spark.implicits._

		val df = List(
			("2018-01-01 11:00:00", "u1"),
			("2018-01-01 12:10:00", "u1"),
			("2018-01-01 13:00:00", "u1"),
			("2018-01-01 13:50:00", "u1"),
			("2018-01-01 14:40:00", "u1"),
			("2018-01-01 15:30:00", "u1"),
			("2018-01-01 16:20:00", "u1"),
			("2018-01-01 16:50:00", "u1"),
			("2018-01-01 11:00:00", "u2"),
			("2018-01-02 11:00:00", "u2")
		).toDF("click_time", "user_id")

		val t1: Long = 60 * 60
		val t2: Long = 2 * 60 * 60

		val win = Window.partitionBy("user_id").orderBy("click_time")

		val df1 = df.withColumn("ts_diff", unix_timestamp($"click_time") - unix_timestamp(lag("click_time", 1).over(win))).
			withColumn("ts_diff", when($"ts_diff".isNull || $"ts_diff" > t1, 0L).otherwise($"ts_diff"))


		val getSessionIdList = udf { (userId: String, clickTimeList: Seq[String], diffTSList: Seq[Long]) => {

			def createSessionId(id: Long) = s"$userId-$id"

			val sessionIDsList = diffTSList.foldLeft((List[String](), 0L, 0L)) { case ((previdList, cumTS, currentId), diffTS) =>

				if (diffTS == 0 || cumTS + diffTS >= t2) {
					(createSessionId(currentId + 1) :: previdList, 0L, currentId + 1)
				} else {
					(createSessionId(currentId) :: previdList, cumTS + diffTS, currentId)
				}
			}._1.reverse

			clickTimeList.zip(sessionIDsList)
		}
		}

		val groupDF = df1.groupBy("user_id").agg(
			collect_list("click_time").as("click_time_list"),
			collect_list("ts_diff").as("ts_diff_list")
		).withColumn("session_id_list", getSessionIdList(col("user_id"), col("click_time_list"), col("ts_diff_list")))

		val explodedDF = groupDF.withColumn("session_id",
			explode(getSessionIdList()(col("user_id"), col("click_time_list"), col("ts_diff_list"))))

		val finalDF = explodedDF.select(col("user_id"), col("session_id._1").as("timestamp"),
			col("session_id._2").as("user_session_id"))

		val sessionperday=  finalDF.withColumn("datecolumn", col("timestamp").cast(DateType)).
			withColumn("sessionperday", size(collect_set("usersessionid").over(Window.partitionBy("datecolumn"))))

		val differ = df.withColumn("diff", col("normalizedTime") - lag(col("normalizedTime"), 1).over(win))

		val  userinfoDF = differ.withColumn("datecolumn", col("timestamp").cast(DateType)).
			withColumn("usertimeperday", sum("diff").over(Window.partitionBy("datecolumn", "userid"))).
			withColumn("yearmonth", year(col("datecolumn"))*100+month(col("datecolumn"))).
			withColumn("usertimepermonth", sum("diff").over(Window.partitionBy("yearmonth", "userid")))

		val beforewriteDF = sessionperday.drop("datecolumn").join(userinfoDF,Seq("userid", "timestamp"),"inner").
			select("userid", "timestamp", "usersessionid", "sessionperday", "usertimeperday", "usertimepermonth",
				"yearmonth", col("datecolumn").toString())


		beforewriteDF.
			write.
			mode("overwrite").
			partitionBy("yearmonth", "datecolumn").
			parquet(destinationPath)
	}
}
