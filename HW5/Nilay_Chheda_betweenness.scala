/**
  * Created by mit2nil on 4/20/17.
  */

import java.io.{File, FileWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext, graphx}
import scala.collection.mutable
import org.apache.commons.math3.util.Precision

object Betweenness {
  def main(args: Array[String]) {

    // Turn off all logs but ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Start timer
    val start = System.currentTimeMillis()

    // Spark context
    val sparkConf = new SparkConf().setAppName("inf553").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // Input/output file path with name
    val infile = args(0)
    val outfile = args(1)

    // Load data set
    val data = sc.textFile(infile)
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
    }).reduceByKey(_+_).filter(x => x._2 >= 3).map(x => Edge(x._1._1, x._1._2, 0.0))

    /*val forwardEdges = sc.parallelize(Array(Edge(1L,2L,0.0), Edge(1L,3L,0.0),
      Edge(2L,3L,0.0), Edge(2L,4L,0.0), Edge(2L,5L,0.0), Edge(3L,5L,0.0),
      Edge(4L,6L,0.0), Edge(4L,7L,0.0), Edge(6L,7L,0.0)))*/

    val edges = forwardEdges.union(forwardEdges.map(x => Edge(x.dstId, x.srcId, 0.0)))

    // Get vertex RDD to initialize the graph
    val vertices = sc.parallelize(forwardEdges.map(x=>Set(x.srcId,x.dstId)).reduce(_++_).map(x=>(x,1.0)).toArray.toSeq)

    // Construct Graph
    val g = Graph(vertices, edges)

    //println("Total edges are : "+g.numEdges)
    //println("Total vertices are : "+g.numVertices)

    // Take nodes and edges in memory for faster computation
    var g_edges = mutable.Map() ++ g.edges.map(x=>((x.srcId,x.dstId),x.attr)).collectAsMap()
    var g_vertices = mutable.Map() ++ g.vertices.collectAsMap()
    val children = g.edges.map(x=>(x.srcId,Set(x.dstId))).reduceByKey(_++_).collectAsMap()

    val g_edges_updated = g.vertices.mapPartitions(itr=>{
      //println("\n")
      for (v <- itr){
        val root = v._1
        var level = 0
        val leafLevel = mutable.HashMap[Int,Set[Long]]()
        val parents = mutable.HashMap[Long,mutable.Set[Long]]()
        //println("Root is : "+root)

        // Run breadth first traversal, reach leaf nodes, come back up by updating edge values
        var prevQ = mutable.Set[graphx.VertexId]()
        var currentQ = mutable.Set[graphx.VertexId]()
        var nextQ = mutable.Set[graphx.VertexId]()

        // Add root to previousQ
        prevQ.add(root)
        parents += root -> mutable.Set()
        //println("Nodes at level "+level+" are "+prevQ.size)

        // Get all edges connected to the node under consideration
        val dest_nodes = g_edges.keys.filter(x=>x._1==root).map(x=>x._2)
        for (dest <- dest_nodes){
          if (parents.contains(dest)){
            val tempSet = parents.apply(dest)
            tempSet.add(root)
            parents += dest -> tempSet
          }
          else{
            parents += dest -> mutable.Set(root)
          }
        }

        // Add all edge destination vertices to nextQ
        nextQ ++= dest_nodes.toSet

        // Keep going one level down until leaf level is reached
        //println("Going down the graph!")
        while(nextQ.nonEmpty){

          // Update level
          level = level + 1
          //println("Nodes at level "+level+" are "+nextQ.size)

          // Transfer nodes from nextQ to currentQ
          currentQ ++= nextQ
          nextQ.clear()

          // Set of nodes at current level
          val c_nodes = mutable.Set[graphx.VertexId]()

          // Pop out one vertex at a time and add its child nodes to nextQ
          while(currentQ.nonEmpty){

            // Dequeue one node from current level
            val d = currentQ.head
            currentQ.remove(d)
            c_nodes.add(d)

            // Get eligible destination nodes
            var dest_nodes = children(d) -- (prevQ ++ currentQ ++ c_nodes)

            for (dest <- dest_nodes){
              if (parents.contains(dest)){
                val tempSet = parents.apply(dest)
                tempSet.add(d)
                parents += dest -> tempSet
              }
              else{
                parents += dest -> mutable.Set(d)
              }
            }
            dest_nodes --= nextQ

            // Transfer nodes to next queue or leaf storage
            if (dest_nodes.nonEmpty){
              nextQ ++= dest_nodes
            }
            else {
              leafLevel += level -> leafLevel.getOrElse(level,Set()).union(Set(d))
            }
          }

          // Transfer nodes from c_nodes to previousQueue
          prevQ = mutable.Set() ++ c_nodes.toSet

        }

        // Transfer prevQ to nextQ (prevQ would contain leafs at bottom most level
        nextQ ++= prevQ
        prevQ.clear()

        // Start from leaf level and go one level up at a time
        //println("Going up the graph!")
        while(nextQ.nonEmpty) {

          // Transfer nodes from nextQ to currentQ
          currentQ ++= nextQ
          nextQ.clear()

          // Transfer nodes from leaf map at different levels
          currentQ ++= leafLevel.getOrElse(level,mutable.Set())

          //println("Nodes at level "+level+" are "+currentQ.size)
          level = level - 1

          // Set of nodes at current level
          val c_nodes = mutable.Set[graphx.VertexId]()

          // Pop out one vertex at a time and add its child nodes to nextQ
          while(currentQ.nonEmpty){

            val d = currentQ.head
            currentQ.remove(d)
            c_nodes.add(d)

            // Use parents hashmap to get eligible edges to visit while going down the graph
            val dest_nodes = parents(d)

            //println("edges connected to node "+d)
            //dest_nodes.foreach(println)

            for (dest <- dest_nodes){

              //  Add dest node to nextQ
              nextQ.add(dest)

              // update destination node value
              val edgeVal = g_vertices(d)/dest_nodes.size
              if (g_vertices.contains(dest)){
                g_vertices += dest -> (g_vertices.apply(dest) + edgeVal)
              }
              else{
                g_vertices += dest -> edgeVal
              }

              // update destination edge value
              //println("incrementing edge ("+dest+","+d+") by value "+edgeVal)
              if (dest < d){
                if (g_edges.contains((dest,d))){
                  g_edges += (dest,d) -> (g_edges.apply((dest,d)) + edgeVal )
                }
                else{
                  g_edges += (dest,d) -> edgeVal
                }
              }
              else{
                if (g_edges.contains((d,dest))){
                  g_edges += (d,dest) -> (g_edges.apply((d,dest)) + edgeVal )
                }
                else{
                  g_edges += (d,dest) -> edgeVal
                }
              }
            }
          }

          // Update previousQ to hold current level nodes
          prevQ = mutable.Set() ++ c_nodes.toSet

          //g_edges.foreach(println)
          //g_vertices.foreach(println)

        }

        // Reset vertex value to 1.0 before starting with new root node
        for(key <- g_vertices){
          g_vertices += key._1 -> 1.0
        }
      }

      // Return iterator to mapPartition
      g_edges.iterator
    }).filter(x=>x._1._1<x._1._2).reduceByKey(_+_).collectAsMap()

    // Sort keys list before dumping out to file
    val keys = g_edges_updated.keys.toArray.sorted

    // Dump to output file
    val writer = new FileWriter(new File(outfile))
    for (key <- keys){
      writer.write("("+key._1+","+key._2+","+g_edges_updated(key)/2+")\n")
    }
    writer.close()

    // Total time
    val end = System.currentTimeMillis()
    println("Took : " + ((end - start) / 1000) +" seconds.")
  }

}
