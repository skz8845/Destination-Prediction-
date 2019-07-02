import java.lang.Math.{abs, acos, atan, cos, pow, sin, sqrt, tan, toRadians, log10}
import java.sql.DriverManager
import java.text.SimpleDateFormat

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
//spark-sql
object knn5 {
  @volatile private var bd_map:Broadcast[Map[String, String]] = null
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
    spark.udf.register("decodeLat", GeoHash.decodeLat(_:String))
    spark.udf.register("decodeLon", GeoHash.decodeLon(_:String))

    val train_data = spark.read.option("header", "true").csv(args(1))
    val test_data = spark.read.option("header", "true").csv(args(2))

    import spark.implicits._
    val train = train_data.map(row => {
      val fields = row.mkString(",").split(",")
      (fields(1), datetime_to_period(fields(2)), date_to_period(fields(2)), fields(4).toDouble, fields(5).toDouble, GeoHash.encode(fields(6).toDouble.formatted("%.5f").toDouble, fields(7).toDouble.formatted("%.5f").toDouble))
    }).toDF("out_id", "hour", "type_of_day", "start_lat", "start_lon", "class")



    val test = test_data.map(row => {
      val fields = row.mkString(",").split(",")
      (fields(0), fields(1), datetime_to_period(fields(2)), date_to_period(fields(2)), fields(3).toDouble.formatted("%.5f").toDouble, fields(4).toDouble.formatted("%.5f").toDouble, fields(5).toDouble, fields(6).toDouble)
    }).toDF("r_key", "out_id2", "hour2", "type_of_day2", "start_lat2", "start_lon2", "end_lat2", "end_lon2")

    train.createOrReplaceTempView("trip_record")
    test.createOrReplaceTempView("test_record")




    spark.sql("cache table trip_record")
    spark.sql("cache table test_record")

    Class.forName("com.mysql.jdbc.Driver")

    val real_location_rdd = spark.sql("select r_key, end_lat2, end_lon2 from test_record").map(t => {
      (t.getString(0), t.getDouble(1) + "," + t.getDouble(2))
    }).rdd.collectAsMap()

    val bd_real_location_rdd = spark.sparkContext.broadcast(real_location_rdd)

    val test_cross_join_train_rdd = spark.sql("select distinct r_key, class from (select r_key, class, row_number() over(partition by r_key order by distance asc, dis desc) as r from (select distinct r_key, class, count(1) over(partition by r_key, class) as dis, distance from (select r_key, class, calc_distance(hour, hour2, type_of_day, type_of_day2, start_lat, start_lon, start_lat2, start_lon2) as distance, rank() over(partition by(r_key) order by calc_distance(hour, hour2, type_of_day, type_of_day2, start_lat, start_lon, start_lat2, start_lon2) asc) as pos from ((select * from test_record) as a join (select * from trip_record) as b on a.out_id2=b.out_id) as tmp) as f where pos <= " + args(0) + ") as z) as c where r=1")

    test_cross_join_train_rdd.foreachPartition(iter1 => {
      val conn = DriverManager.getConnection("jdbc:mysql://node1:3306/mydb", "root", "root")
      val st = conn.createStatement()
      iter1.foreach(iter2 => {
        val r_key_loc = bd_real_location_rdd.value(iter2.getString(0)).split(",")
        if (!(r_key_loc(0).toDouble.isNaN || r_key_loc(1).toDouble.isNaN)) {
          val geode = GeoHash.decode(iter2.getString(1))
          if (!geode(0).isNaN && !geode(1).isNaN) {
            val dis = getDistance(geode(0), geode(1), r_key_loc(0).toDouble, r_key_loc(1).toDouble)
            if(!dis.isNaN){
                st.executeUpdate("insert into u_trip_res(r_key, lat, lon, dis, k) values('" + iter2.getString(0) + "', " + geode(0) + ", " + geode(1) + ", " + dis + ", " + args(0) + ")")
            }
          }
        }
      })
      st.close()
      conn.close()
    })
    spark.close()
  }
}
