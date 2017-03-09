/**
  * Created by mit2nil on 1/27/17.
  */

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

object Nilay_Chheda_task1 {

  def main(args: Array[String]){
    val sparkConf = new SparkConf().setAppName("inf553").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val s = scala.util.Properties.envOrElse("DATADIR", "")
    var csv = sc.textFile(s.concat("ratings.csv"))
    val header = csv.first()
    csv = csv.filter(line => line != header)

    csv.map(line => (line.split(",")(1).toInt, line.split(",")(2).toDouble))
        .mapValues(x=>(x,1))
        .reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
        .map{case (k, v) => (k, v._1 / v._2.toFloat)}
        .sortByKey().coalesce(1,true)
        .saveAsTextFile(s.concat("task1"))

    if (s.contains("ml-20m")){
      new File(s.concat("task1"+java.io.File.separator+"part-00000")).renameTo(new File(s.concat("Nilay_Chheda_result_task1_big")))
    }
    else{
      new File(s.concat("task1"+java.io.File.separator+"part-00000")).renameTo(new File(s.concat("Nilay_Chheda_result_task1_small")))
    }
    FileUtils.deleteDirectory(new File(s.concat("task1")))
  }
}
