import java.io.{File, FileWriter}
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge

import scala.collection.mutable
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
  * Created by mit2nil on 4/28/17.
  */
object Bonus {

  def main(args: Array[String]) {

    // Turn off all logs but ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Start timer
    val start = System.currentTimeMillis()

    // Spark context
    val sparkConf = new SparkConf().setAppName("inf553").setMaster("local[*]")
    implicit val ctx:SparkContext = new SparkContext(sparkConf)

    // Input/output file path with name
    val infile = args(0)
    val outfile = args(1)

    // Load data set
    val data = ctx.textFile(infile)
    val data_header = data.first()

    val movieToUsers = data.filter(x => x != data_header)
      .map(x => (x.split(',')(1).toInt, Set(x.split(',')(0).toInt)))
      .reduceByKey((x, y) => x ++ y)

    val forwardEdges = movieToUsers.mapPartitions(x => {

      val userpairs = mutable.Map[(Int, Int), Int]()

      // Run over iterator
      for (userList <- x) {

        // Get list of users and emit pair of users
        val users = userList._2.toList.sorted
        for (i <- users.indices) {
          for (j <- i + 1 until users.size) {
            userpairs += (users(i), users(j)) -> (userpairs.getOrElse((users(i), users(j)), 0) + 1)
          }
        }
      }
      userpairs.iterator
    }).reduceByKey(_ + _).filter(x => x._2 >= 3).map(x => Edge(x._1._1, x._1._2, 0.0))

    /*val forwardEdges = ctx.parallelize(Array(Edge(1L,2L,0), Edge(1L,3L,0),
      Edge(2L,3L,0), Edge(2L,4L,0), Edge(2L,5L,0), Edge(3L,5L,0),
      Edge(4L,6L,0), Edge(4L,7L,0), Edge(6L,7L,0)))*/

    val edges = forwardEdges.union(forwardEdges.map(x => Edge(x.dstId, x.srcId, 0)))

    // Get vertex RDD to initialize the graph
    val vertices = ctx.parallelize(forwardEdges.map(x => Set(x.srcId, x.dstId)).reduce(_ ++ _).map(x => (x, 0)).toArray.toSeq)

    // Construct Graph
    val g = Graph(vertices, edges)
    // Graph where each vertex is associated with its component identifier
    val components = g.PSCAN(epsilon=0.5).vertices.map(x=>(x._2,mutable.Set(x._1))).reduceByKey(_++_).collectAsMap()

    // Dump to output file
    val keys = components.keys.toList.sorted
    val writer = new FileWriter(new File(outfile))

    for(key <- keys){
      val out = components(key).toList.sorted
      //println(components(key).toString())
      writer.write("["+out.head)
      for(i <- 1 until out.length) yield writer.write(","+out(i))
      writer.write("]\n")
    }
    writer.close()
  }

}
