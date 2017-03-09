/**
  * Created by mit2nil on 1/27/17.
  */

import java.io.{File, PrintWriter}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

object Nilay_Chheda_task2 {

  def main(args: Array[String]){
    val sparkConf = new SparkConf().setAppName("inf553").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val s = scala.util.Properties.envOrElse("DATADIR", "")
    var csv1 = sc.textFile(s.concat("ratings.csv"))
    val header1 = csv1.first()
    csv1 = csv1.filter(line => line != header1)

    val ratings = csv1.map(line => (line.split(",")(1).toInt, line.split(",")(2).toDouble))
        .mapValues(x=>(x,1))
        .reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
        .map{case (k, v) => (k, (v._1, v._2.toFloat))}

    var csv2 = sc.textFile(s.concat("tags.csv"))
    val header2 = csv2.first()
    csv2 = csv2.filter( line => line != header2)

    val ratingsMap = ratings.collectAsMap()

    csv2.map(line => (line.split(",")(2), line.split(",")(1).toInt))
         .mapValues(x=>(x,ratingsMap.getOrElse(x,(0.0,0.0f))._1,ratingsMap.getOrElse(x,(0.0,0.0f))._2))
         .reduceByKey((x,y)=>(1, x._2+y._2, x._3+y._3))
         .map{case (k,v) => (k,v._2/v._3.toFloat)}
         .sortByKey(false).coalesce(1,true)
         .saveAsTextFile(s.concat("task2"))

    if (s.contains("ml-20m")){
      val writer = new PrintWriter(new File(s.concat("Nilay_Chheda_result_task2_big.csv" )))
      writer.write("tag,rating_avg\n")
      for (line <- Source.fromFile(s.concat("task2"+java.io.File.separator+"part-00000")).getLines) {
        writer.write(line.substring(1,line.length-1)+"\n")
      }
      writer.close()
    }
    else{
      val writer = new PrintWriter(new File(s.concat("Nilay_Chheda_result_task2_small.csv" )))
      writer.write("tag,rating_avg\n")
      for (line <- Source.fromFile(s.concat("task2"+java.io.File.separator+"part-00000")).getLines) {
        writer.write(line.substring(1,line.length-1)+"\n")
      }
      writer.close()
    }
    FileUtils.deleteDirectory(new File(s.concat("task2")))
  }
}
