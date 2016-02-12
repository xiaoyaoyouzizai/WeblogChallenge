
import org.apache.spark._
import org.apache.spark.SparkContext._

object Weblog {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: org.paytmlabs.Weblog <input> <output> <output>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Weblog");
    val sc = new SparkContext(conf)

    val min15 = 15 * 60 * 1000
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    //"file:///home/cloudera/2015_07_22_mktplace_shop_web_log_sample.log"
    val rdd1 = sc.textFile(args(0))
    val rdd2 = rdd1.map(_.split(" "))
    val rdd3 = rdd2.map { x =>
      var index = 0
      var time = 0L
      var ip = ""
      var url = ""
      x.map { y =>
        index += 1
        if (index == 1) {
          time = sdf.parse(y.substring(0, y.indexOf("."))).getTime()
        } else if (index == 3) {
          val i = y.indexOf(":")
          if (i < 0) {
            ip = y
          } else {
            ip = y.substring(0, i)
          }
        } else if (index == 13) {
          val i = y.indexOf("?")
          if (i < 0) {
            url = y
          } else {
            url = y.substring(0, i)
          }
        }
      }
      (ip, time, url)
    }

    val rdd4 = rdd3.groupBy(_._1).mapValues { x =>

      var sessionId = 0L
      var lastTime = 0L
      x.toSeq.sortBy(_._2).map { y =>

        if (y._2 - lastTime > min15) {
          sessionId += 1
        }
        lastTime = y._2
        (sessionId, y._2, y._3)
      }

    }
    //1 rdd5 is RDD of Sessionize the web log
    val rdd5 = rdd4.mapValues { x =>
      x.groupBy(_._1)
    }

    val rdd6 = rdd5.mapValues { x =>
      x.mapValues { y =>
        val max = y.maxBy(_._2)
        val min = y.minBy(_._2)
        max._2 - min._2
      }.map(identity)
    }.map(identity)

    val rdd7 = rdd6.mapValues { x =>
      (x.size, x.map(_._2).sum)
    }
    //2 averageSessionTime by minute
    val averageSessionTime = rdd7.map(_._2._2).sum / rdd7.map(_._2._1).sum / 1000 / 60
    println("Average Session Time (minute) :" + averageSessionTime)

    val rdd8 = rdd5.mapValues { x =>
      x.mapValues { y =>
        y.groupBy(_._3)
      }.map(identity)
    }.map(identity)
    //3 Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    val rdd9 = rdd8.mapValues { x =>
      x.mapValues { y =>
        y.mapValues { m =>
          m.size
        }.map(identity)
      }.map(identity)
    }.map(identity)
    rdd9.saveAsTextFile(args(1))

    val rdd10 = rdd6.mapValues { x =>

      x.maxBy(_._2)

    }.map(identity)

    val rdd11 = rdd10.map(x => (x._1 -> x._2._2)).sortBy(_._2, false)

    //The top 10 of most engaged users
    val rdd12 = rdd11.take(10)
    println("The top 10 of most engaged users:")
    rdd12.saveAsTextFile(args(2))
  }
}
