package my.spark

import java.net.URLDecoder

import org.apache.spark.{SparkConf, SparkContext}

object BasicWordCount {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("Basic WordCount").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val textFile = sc.textFile("log/2016list.txt")
    println(textFile.count())

    for ((line, idx) <- textFile.zipWithIndex) {
      //if (idx < 100) {
      try {
        val decodedLine = URLDecoder.decode(line, "utf8")
        val elems = decodedLine split '|' filter (e => !e.isEmpty)
        val requests = elems(10) split " "
        //println(requests(1).split('?')(1))
      } catch {
        case e:Exception => {
          //println(e)
//          val bytes: Array[Byte] = line.getBytes("UTF-8")
//          val s: String = new String(bytes, "UTF-8")
          println(unicodeDecode(line))
        }
      }
      //}
    }

//    val words = textFile.flatMap(line => line.split(" "))
//    val wordCounts = words.map(word => (word, 1)).reduceByKey((a, b) => a + b)
//
//    wordCounts.saveAsTextFile("./wordcounts")
  }

  def unicodeDecode(str: String): String = {
    val parts = """%u\d{4}|%\d\d|[^%]+""".r.findAllIn(str).map(s =>
      if(s.startsWith("%")) {
        Integer.parseInt(
          (if(s.startsWith("%u")) s.substring(2, s.size)
          else s.substring(1)), 16).toChar.toString
      } else s)
    parts.mkString
  }
}

