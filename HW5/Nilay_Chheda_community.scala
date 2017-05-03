/**
  * Created by mit2nil on 4/20/17.
  */

import java.io.{File, FileWriter}

import org.apache.commons.math3.util.Precision
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext, graphx}

import scala.collection.mutable

object Community {

  def canSplit(children: mutable.Map[Long,mutable.Set[Long]],root:Long, key: Long) : Boolean = {

    // Run BFS to find list of all reachable vertices
    var cQ = mutable.Set(root)
    var nQ = children(root).clone()
    var rc = true
    if (!nQ.contains(key)){
      while(nQ.nonEmpty){

        // Remove head from the neighbours
        val node = nQ.head
        nQ.remove(node)
        cQ.add(node)

        //println("Node - Key "+node+"-"+key)

        if (node != key){
          // Add node's children to nextQ
          for(child <- children(node)){
            if (!cQ.contains(child)){
              nQ.add(child)
            }
          }
        }
        else {
          nQ.clear()
          rc = false
        }
      }
    }

    /*if (rc){
      println("Root "+root+" is not reachable from "+key+" - can split")
    }
    else{
      println("Root "+root+" is reachable from "+key+" - cant split")
    }*/
    rc
  }

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
      Edge(2L,3L,0.0), Edge(2L,4L,0.0), Edge(4L,5L,0.0), Edge(4L,6L,0.0),
      Edge(4L,7L,0.0), Edge(5L,6L,0.0), Edge(6L,7L,0.0)))*/

    /*val forwardEdges = sc.parallelize(Array(Edge(1L,2L,0.0), Edge(1L,3L,0.0),
      Edge(2L,3L,0.0), Edge(3L,7L,0.0), Edge(6L,7L,0.0), Edge(4L,6L,0.0),
      Edge(5L,6L,0.0), Edge(4L,5L,0.0), Edge(7L,8L,0.0), Edge(8L,9L,0.0), Edge(9L,10L,0.0),
      Edge(9L,11L,0.0), Edge(10L,11L,0.0), Edge(8L,12L,0.0), Edge(7L,8L,0.0), Edge(12L,13L,0.0),
      Edge(13L,14L,0.0), Edge(12L,14L,0.0)))*/

    val edges = forwardEdges.union(forwardEdges.map(x => Edge(x.dstId, x.srcId, 0.0)))

    // Get vertex RDD to initialize the graph
    val vertices = sc.parallelize(forwardEdges.map(x=>Set(x.srcId,x.dstId)).reduce(_++_).map(x=>(x,1.0)).toArray.toSeq)

    // Construct Graph
    val g = Graph(vertices, edges)

    val edgeCount = g.numEdges.toDouble
    val vertexCount = g.numVertices.toDouble
    println("Total edges are : "+edgeCount)
    println("Total vertices are : "+vertexCount)

    // Take nodes and edges in memory for faster computation
    var g_edges = mutable.Map() ++ g.edges.map(x=>((x.srcId,x.dstId),x.attr)).collectAsMap()
    var g_vertices = mutable.Map() ++ g.vertices.collectAsMap()
    var children = mutable.Map() ++ g.edges.map(x=>(x.srcId,mutable.Set(x.dstId))).reduceByKey(_++_).collectAsMap()

    var g_edges_updated = g.vertices.mapPartitions(itr=>{
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
    }).filter(x=>x._1._1<x._1._2).reduceByKey(_+_).mapValues(x=>Precision.round(x/2,1))
      .map(x=>(x._2,Set(x._1))).reduceByKey(_++_).collectAsMap()


    // ========================================================================
    //                        Modularity Calculation
    // ========================================================================

    // Create sorted list of betweenness keys
    var sorted_betweenness = g_edges_updated.keys.toArray.sorted(Ordering[Double].reverse)

    //sorted_betweenness.foreach(x=>println("Key: "+x+" value: "+g_edges_updated(x)))

    // Calculate and store original graph degrees before graph is updated
    var degrees = mutable.Map[Long,Int]()
    for(key <- children.keys){
      degrees += key -> children(key).size
    }

    // Initialize modularity
    var modularity = 0.0

    // Update modularity by adding (1/0 - k_i*k_j/m)
    for(i <- children.keys) {
      // Diagonal element aka self edge
      modularity += 1 - (degrees(i) * degrees(i)) / edgeCount
      for (j <- children.keys) {
        if (i != j) {
          if (children(i).contains(j)) {
            // Edges with direct connection
            modularity += 1 - (degrees(i) * degrees(j)) / edgeCount
          }
          else {
            // Edges with no direct connection but in same community
            modularity += 0 - (degrees(i) * degrees(j)) / edgeCount
          }
        }
      }
    }
    modularity /= edgeCount
    //println("Initial modularity is - "+modularity)

    // To find local maxima
    var prev_modularity = modularity

    var edge_cuts = mutable.Set[(Long,Long)]()
    var max_edge_cuts = mutable.Set[(Long,Long)]()
    var finalGraph = children.clone()

    // Run loop over keys of betweenness
    var b_i = 0
    var prev_components = 0
    while(b_i < sorted_betweenness.length){

      //println("")
      //sorted_betweenness.foreach(x=>println("Key: "+x+" value: "+g_edges_updated_inverted(x)))
      //println("\n")

      // Get highest betweenness edge(s)
      var edges = g_edges_updated(sorted_betweenness(b_i))

      g_edges_updated -= sorted_betweenness(b_i)

      // increment index for next iteration
      b_i += 1

      //println("Deleting "+edges.size+" edges")
      for(delete_edge <- edges){
        //println("Deleting edge from both direction : "+delete_edge)

        // Remove edge from children Map and update it back

        val rightChildren = mutable.Set() ++ children(delete_edge._1) -- mutable.Set(delete_edge._2)
        children.update(delete_edge._1, rightChildren)

        val leftChildren = mutable.Set() ++ children(delete_edge._2) -- mutable.Set(delete_edge._1)
        children.update(delete_edge._2, leftChildren)

        // Run modularity calculations only if there was a split
        if (canSplit(children, delete_edge._1,delete_edge._2)){

          //println("\nFound a split after deleting : "+delete_edge+" with betweenness value "+sorted_betweenness(b_i-1))

          // Compute modularity and check with
          var current_modularity = 0.0
          // Set to keep track of visited node for BFS
          var notVisited = mutable.Set() ++ g_vertices.keys.toSet
          // Start from random root value
          var root = 0L
          // Number of components in this split
          var no_components = 0
          // Run until every node is visited
          while (notVisited.nonEmpty){

            // Find next root for BFS
            root = notVisited.head

            // Run BFS to find list of all reachable vertices
            var connected_vertices = mutable.Set(root)
            var neighbours = children(root).clone()

            while(neighbours.nonEmpty){

              //println("visiting nodes : "+neighbours.toString())

              // Remove head from the neighbours
              val node = neighbours.head
              connected_vertices.add(node)
              neighbours.remove(node)
              //println("visited node : "+node)

              // Find all the children of removed neighbour
              for(child <- children(node)){
                // Add it to connected list if doesn't exist
                if (!connected_vertices.contains(child)){
                  // Move children of neighbours (next level) to the nextQ
                  neighbours ++= mutable.Set(child)
                }
              }
            }

            if (connected_vertices.nonEmpty){
              // increment no_component count
              no_components += 1
            }

            //println("Connected component is : "+connected_vertices.toString())
            //println("Total vertices in this component : "+connected_vertices.size)

            // Remove all vertices discovered from not Visited
            notVisited --= connected_vertices

            // Update modularity by adding (1/0 - k_i*k_j/m)
            for(i <- connected_vertices){
              // Diagonal element aka self edge
              current_modularity += 1 - (degrees(i)*degrees(i))/edgeCount
              for(j <- connected_vertices){
                if (i != j){
                  if (children(i).contains(j)){
                    // Edges with direct connection
                    current_modularity += 1 - (degrees(i)*degrees(j))/edgeCount
                  }
                  else{
                    // Edges with no direct connection but in same community
                    current_modularity += 0 - (degrees(i)*degrees(j))/edgeCount
                  }
                }
              }
            }
          }

          // divide modularity by 2*m for normalized value
          current_modularity /= edgeCount

          //println("Total "+no_components+" connected components")

          // Update edge list if new max moduarity is found
          edge_cuts.add(delete_edge)
          if (modularity < current_modularity){

            //println("max modularity increased from "+modularity+" to "+current_modularity)

            // Update max modularity value and edge list to be cut
            modularity = current_modularity
            max_edge_cuts.clear()
            max_edge_cuts = edge_cuts.clone()
          }
          else{
            //println("current modularity is "+current_modularity)
          }

          if (prev_modularity < current_modularity){
            //println("current modularity increased from "+prev_modularity+" to "+current_modularity)
          }
          prev_modularity = current_modularity

        }

      }

    }

    //println("Deleting edges from original graph")
    //max_edge_cuts.foreach(println)

    // Remove all edges from original graph
    for(e <- max_edge_cuts){
      finalGraph += e._1 -> (finalGraph.getOrElse(e._1,mutable.Set()) -- mutable.Set(e._2))
      finalGraph += e._2 -> (finalGraph.getOrElse(e._2,mutable.Set()) -- mutable.Set(e._1))
    }

    // Store sorted communities
    var communities = mutable.HashMap[Long,Array[Long]]()

    // Set to keep track of visited node for BFS
    var notVisited = mutable.Set() ++ g_vertices.keys.toSet
    // Start from random root value
    var root = 0L
    // Number of components in this split
    var no_components = 0
    // Run until every node is visited
    while (notVisited.nonEmpty){

      // Find next root for BFS
      root = notVisited.head

      // Run BFS to find list of all reachable vertices
      var connected_vertices = mutable.Set(root)
      var neighbours = finalGraph(root).clone()

      while(neighbours.nonEmpty){

        //println("visiting nodes : "+neighbours.toString())

        // Remove head from the neighbours
        val node = neighbours.head
        connected_vertices.add(node)
        neighbours.remove(node)
        //println("visited node : "+node)

        // Find all the children of removed neighbour
        for(child <- finalGraph(node)){
          // Add it to connected list if doesn't exist
          if (!connected_vertices.contains(child)){
            // Move children of neighbours (next level) to the nextQ
            neighbours ++= mutable.Set(child)
          }
        }
      }

      if (connected_vertices.nonEmpty){
        // increment no_component count
        no_components += 1
      }

      //println("Connected component is : "+connected_vertices.toString())
      //println("Total vertices in this component : "+connected_vertices.size)

      // Remove all vertices discovered from not Visited
      notVisited --= connected_vertices

      val out = connected_vertices.toArray.sorted
      communities += out.head -> out

    }

    // Dump to output file
    val writer = new FileWriter(new File(outfile))
    val communityHeads = communities.keys.toArray.sorted
    for(key <- communityHeads){
      val out = communities(key)
      writer.write("["+out.head)
      for(i <- 1 until out.length) yield writer.write(","+out(i))
      writer.write("]\n")
    }
    // Close the file
    writer.close()

    // Total time
    val end = System.currentTimeMillis()
    println("Took : " + ((end - start) / 1000) +" seconds.")
  }

}
