import java.lang.Math._
import java.sql.DriverManager
import java.text.SimpleDateFormat

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

object bayes2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("knn" + args(0))
      .getOrCreate()

    def datetime_to_period(date_str: String): Int = {
      val time_part = date_str.split(" ")(1)
      time_part.split(":")(0).toInt
    }

    def date_to_period(date_str: String): Int = {
      val holiday_list = Array("2018-01-01", "2018-02-15", "2018-02-16", "2018-02-17", "2018-02-18", "2018-02-19", "2018-02-20", "2018-02-21", "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-29", "2018-04-30", "2018-05-01", "2018-06-16", "2018-06-17", "2018-06-18") // 小长假
      val switch_workday_list = Array("2018-02-11", "2018-02-24", "2018-04-08", "2018-04-28") // 小长假补班
      val workday_list = Array("星期一", "星期二", "星期三", "星期四", "星期五") // 周一到周五
      val weekday_list = Array("星期日", "星期六") // 周六、周日，其中0表示周日
      val date = date_str.split(" ")(0) // 获取日期部分
      val sdf1 = new SimpleDateFormat("E")
      val sdf2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val whatday = sdf1.format(sdf2.parse(date_str))
      if (holiday_list.contains(date))
        2
      else if (switch_workday_list.contains(date))
        0
      else if (workday_list.contains(whatday))
        0
      else if (weekday_list.contains(whatday))
        1
      else
        0
    }

    def getDistance(latA: Double, lonA: Double, latB: Double, lonB: Double): Double = {
      val ra = 6378140 // radius of equator: meter
      val rb = 6356755 // radius of polar: meter
      val flatten = (ra - rb) / ra // Partial rate of the earth
      // change angle to radians
      val radLatA = toRadians(latA)
      val radLonA = toRadians(lonA)
      val radLatB = toRadians(latB)
      val radLonB = toRadians(lonB)

      try {

        val pA = atan(rb / ra * tan(radLatA))
        val pB = atan(rb / ra * tan(radLatB))
        val x = acos(sin(pA) * sin(pB) + cos(pA) * cos(pB) * cos(radLonA - radLonB))
        val c1 = pow((sin(x) - x) * (sin(pA) + sin(pB)), 2) / pow(cos(x / 2), 2)
        val c2 = pow((sin(x) + x) * (sin(pA) - sin(pB)), 2) / pow(sin(x / 2), 2)
        val dr = flatten / 8 * (c1 - c2)
        val distance = ra * (x + dr)
        distance // meter
      }
      catch {
        case e: Exception => {
          0.0000001
        }
      }
    }

    def calc_distance(h: String, h2: String, d: String, d2: String, ata: String,
                      ona: String, atb: String, onb: String): Double = {
      val hour = h.toInt
      val hour2: Int = h2.toInt
      val day: Int = d.toInt
      val day2: Int = d2.toInt
      val lata: Double = ata.toDouble
      val lona: Double = ona.toDouble
      val latb: Double = atb.toDouble
      val lonb: Double = onb.toDouble
      var sum: Double = 0

      sum = sum + pow(abs(hour - hour2), 2)

      sum = sum + pow(abs(day - day2), 2)

      sum = sum + log10(pow(getDistance(lata, lona, latb, lonb), 2))

      sqrt(sum)
    }

    spark.udf.register("datetime_to_period", datetime_to_period(_: String))
    spark.udf.register("date_to_period", date_to_period(_: String))
    spark.udf.register("getDistance", getDistance(_: Double, _: Double, _: Double, _: Double))
    spark.udf.register("calc_distance", calc_distance(_: String, _: String, _: String, _: String, _: String, _: String, _: String, _: String))
    spark.udf.register("decodeLat", GeoHash.decodeLat(_: String))
    spark.udf.register("decodeLon", GeoHash.decodeLon(_: String))

    val train_data = spark.read.option("header", "true").csv(args(1))
    val test_data = spark.read.option("header", "true").csv(args(2))

    import spark.implicits._
    val train = train_data.map(row => {
      val fields = row.mkString(",").split(",")
      (fields(1), fields(6).toDouble.formatted("%.5f").toDouble * 1e8 + fields(7).toDouble.formatted("%.5f").toDouble, datetime_to_period(fields(2)), date_to_period(fields(2)), fields(4).toDouble, fields(5).toDouble)
    }).toDF("out_id", "class", "hour", "type_of_day", "start_lon", "start_lat")

    val test = test_data.map(row => {
      val fields = row.mkString(",").split(",")
      (fields(0), fields(1), datetime_to_period(fields(2)), date_to_period(fields(2)), fields(3).toDouble.formatted("%.5f").toDouble, fields(4).toDouble.formatted("%.5f").toDouble, fields(5).toDouble, fields(6).toDouble)
    }).toDF("r_key", "out_id2", "hour2", "type_of_day2", "start_lat2", "start_lon2", "end_lat2", "end_lon2")

    test.cache()
    train.cache()

    val test_out_id = test.select("out_id2").distinct().collect()

    val bd_test_out_id = spark.sparkContext.broadcast(test_out_id)

    bd_test_out_id.value.foreach(iter1 => {
      val train_out_id = train.filter($"out_id" === iter1.getString(0))
      val model = NaiveBayes.train(train_out_id.map(t => LabeledPoint(t.getDouble(1), Vectors.dense(t.getInt(2), t.getInt(3), t.getDouble(4), t.getDouble(5)))).rdd, lambda = 1.0, modelType = "multinomial")
      val test_2_predict = test.filter($"out_id2" === iter1.getString(0))

      test_2_predict.foreachPartition(iter2 => {
        val conn = DriverManager.getConnection("jdbc:mysql://node1:3306/mydb", "root", "root")
        val st = conn.createStatement()
        iter2.foreach(iter3 => {
          try {
            val res = model.predict(Vectors.dense(iter3.getInt(2), iter3.getInt(3), iter3.getDouble(4), iter3.getDouble(5)))
            val lat = (res / 1e8).toDouble.formatted("%.5f").toDouble
            val lon = (res % 1e3).toDouble.formatted("%.5f").toDouble
            val dis = getDistance(lat, lon, iter3.getDouble(6), iter3.getDouble(7))
            st.executeUpdate("insert into u_trip_res(r_key, lat, lon, dis, k, type) values('" + iter3.getString(0) + "', " + lat + ", " + lon + ", " + dis + ", " + args(0) + ", 'bayes_m')")

          } catch {
            case exception: Exception => {
            }
          }
        })

        st.close()
        conn.close()
      })
    })
    spark.close()
  }
}
