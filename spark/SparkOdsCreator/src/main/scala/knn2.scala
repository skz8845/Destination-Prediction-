import java.io.{BufferedWriter, FileWriter}
import java.lang.Math._
import java.sql.DriverManager
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
//优先队列
object knn2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("knn" + args(0))
      .getOrCreate()

    def datetime_to_period(date_str : String):Int = {
      val time_part = date_str.split(" ")(1)
      time_part.split(":")(0).toInt
    }
    def date_to_period(date_str : String): Int = {
      val holiday_list = Array("2018-01-01", "2018-02-15", "2018-02-16", "2018-02-17", "2018-02-18", "2018-02-19", "2018-02-20", "2018-02-21", "2018-04-05", "2018-04-06", "2018-04-07", "2018-04-29", "2018-04-30", "2018-05-01", "2018-06-16", "2018-06-17", "2018-06-18")  // 小长假
      val switch_workday_list = Array("2018-02-11", "2018-02-24", "2018-04-08", "2018-04-28")  // 小长假补班
      val workday_list = Array("星期一", "星期二", "星期三", "星期四", "星期五")  // 周一到周五
      val weekday_list = Array("星期日", "星期六")  // 周六、周日，其中0表示周日
      val date = date_str.split(" ")(0)  // 获取日期部分
      val sdf1 = new SimpleDateFormat("E")
      val sdf2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val whatday = sdf1.format(sdf2.parse(date_str))
      if(holiday_list.contains(date))
        2
      else if(switch_workday_list.contains(date))
        0
      else if(workday_list.contains(whatday))
        0
      else if(weekday_list.contains(whatday))
        1
      else
        0
    }
    def getDistance(latA:Double, lonA:Double, latB:Double, lonB:Double):Double = {
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
    def calc_distance(h : Int, h2 : Int, d : Int, d2 : Int, ata : String,
                      ona : String, atb : String, onb : String):Double = {
      val hour = h.toInt
      val hour2:Int = h2.toInt
      val day:Int = d.toInt
      val day2:Int = d2.toInt
      val lata:Double = ata.toDouble
      val lona:Double = ona.toDouble
      val latb:Double = atb.toDouble
      val lonb:Double = onb.toDouble
      var sum: Double = 0

      sum = sum + log10(pow(abs(hour - hour2), 2))

      sum = sum + log10(pow(abs(day - day2), 2))

      sum = sum + log10(pow(getDistance(lata, lona, latb, lonb), 2))

      sum

    }



    spark.udf.register("datetime_to_period", datetime_to_period(_:String))
    spark.udf.register("date_to_period", date_to_period(_:String))
//    spark.udf.register("calc_distance", calc_distance(_:String, _:String, _:String, _:String, _:String, _:String, _:String, _:String))

    var train = spark.read.option("header", "true").csv(args(1))
    var test = spark.read.option("header", "true").csv(args(2))

    train.createOrReplaceTempView("trip_record")
    test.createOrReplaceTempView("test_record")

    val train_data = spark.sql("select start_lat, start_lon, datetime_to_period(start_time) as hour, date_to_period(start_time) as type_of_day, end_lat, end_lon, out_id from trip_record").collect()
    val bd_train_data = spark.sparkContext.broadcast(train_data)

    val test_data = spark.sql("select r_key, start_lat as start_lat2, start_lon as start_lon2, datetime_to_period(start_time) as hour2, date_to_period(start_time) as type_of_day2, out_id as out_id2, end_lat as end_lat2, end_lon as end_lon2 from test_record").cache()
    implicit val pqOrdering = new Ordering[(String, String, String, Double, Int)] {
      override def compare(x: (String, String, String, Double, Int), y: (String, String, String, Double, Int)): Int = {
        if (x._5 == y._5)
          return x._4.compareTo(y._4)
        else
          return -x._5.compareTo(y._5)
      }
    }
    test_data.foreach(iter1 => {
      var pq = new mutable.PriorityQueue[(String, String, String, Double, Int)]()
      try {
        bd_train_data.value.filter(_.getString(6).equals(iter1.getString(5))).foreach(iter3 => {
          val distance = calc_distance(iter1.getInt(3), iter3.getInt(2), iter1.getInt(4), iter3.getInt(3),iter1.getString(1), iter1.getString(2),iter3.getString(0).toDouble.formatted("%.5f"),iter3.getString(1).toDouble.formatted("%.5f"))
          val tpq = new mutable.PriorityQueue[(String, String, String, Double, Int)]()
          var flag = false
          pq.foreach(elem => {
            if (elem._1.equals(iter3.getString(0)) && elem._2.equals(iter3.getString(4)) && elem._3.equals(iter3.getString(5))) {
              tpq.enqueue((elem._1, elem._2, elem._3, elem._4, elem._5 + 1))
              flag = true
            }
            else tpq.enqueue(elem)
          })
          pq = tpq
          if (!flag && pq.length < args(0).toInt) {

            pq.enqueue((iter1.getString(0), iter3.getString(4).toDouble.formatted("%.5f"), iter3.getString(5).toDouble.formatted("%.5f"), distance, 1))
          }
          else if (!flag && pq.length >= args(0).toInt){
            val temp = pq.dequeue()
            pq += (if(temp._4 < distance) temp else (iter1.getString(0), iter3.getString(4), iter3.getString(5), distance, 1))
          }
        })
        val elem = pq.dequeue()
        pq.clear()
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://node1:3306/mydb", "root", "root")
        val st = conn.createStatement()
        st.executeUpdate("insert into u_trip_res values(null, '" + elem._1 + "'" + ", " + elem._2 + ", " + elem._3 + ", " + args(0) + ", " + getDistance(iter1.getString(6).toDouble, iter1.getString(7).toDouble, elem._2.toDouble, elem._3.toDouble) + ", 'queue')")
        st.close()
        conn.close()
      }catch {
        case e: Exception => {

        }
      }
      })

    spark.stop()
  }
}
