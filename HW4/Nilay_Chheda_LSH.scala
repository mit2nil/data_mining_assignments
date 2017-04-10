import java.io.{File, FileWriter}

import org.apache.commons.math3.util.Precision
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by mit2nil on 4/2/17.
  */
object Nilay_Chheda_LSH {
  def main(args: Array[String]) {

    // Turn off logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark context
    val sparkConf = new SparkConf().setAppName("inf553").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // Output file logic
    val datafile = args(0)
    val outfile = args(1)

    // Load data set
    var data = sc.textFile(datafile)
    val data_header = data.first()
    data = data.filter(x => x != data_header)

    //data.take(5).foreach(println)

    // Create movieId
    val movies = data.map(x => x.split(',')(1).toInt).distinct().collect().sorted
    val users = data.map(x => x.split(',')(0).toInt).distinct().collect()

    //movies.take(5).foreach(println)
    //users.take(5).foreach(println)

    // Create four maps: Map(MovieId -> Index), Map(Index -> MovieId), Map(UserId -> Index), Map(Index -> UserId)
    val movieToIndex = mutable.Map[Int,Int]()
    val indexToMovie = mutable.Map[Int,Int]()
    var count = 0
    movies.foreach(x =>{
      if (!movieToIndex.contains(x)){
        movieToIndex += (x -> count)
        count = count + 1
      }
    })
    for (k <- movieToIndex.keys){
      indexToMovie += movieToIndex(k) -> k
    }
    println("Total number of unique movies : "+movieToIndex.size)

    val userToIndex = mutable.Map[Int,Int]()
    val indexToUser = mutable.Map[Int,Int]()
    count = 0
    users.foreach(x => {
      if (!userToIndex.contains(x)){
        userToIndex += (x -> count)
        count = count + 1
      }
    })
    for(k <- userToIndex.keys){
      indexToUser += (userToIndex(k) -> k)
    }
    println("Total number of unique users : "+userToIndex.size)

    // Create list of sparse vectors with each vector representing user list of signature matrix
    // RDD[MovieID -> Iterator(UserIds)]
    val userVector = data.map(x=>(movieToIndex(x.split(',')(1).toInt),userToIndex(x.split(',')(0).toInt))).groupByKey().collect()
    val characteristic = Array.ofDim[Int](movieToIndex.size, userToIndex.size)

    userVector.foreach(x => {
      val temp = Array.fill(userToIndex.size)(0)
      for( index <- x._2){
        temp(index) = 1
      }
      characteristic(x._1) = temp
    })

    //characteristic(0).foreach(print)
    //println(characteristic(0).size)
    //println(characteristic.size)

    // Create hash functions
    val noHash = 10
    //val hashA = Array.fill[Int](noHash)(1)
    //val hashB = Array.fill[Int](noHash)(1)

    val hashA = Array(80,31,51,11,64,19,67,49,10,89)
    val hashB = Array(51,69,99,75,40,76,66,22,54,10)

    /*
    val r = scala.util.Random
    for(h <- 0 until noHash){
      var a = userToIndex.size
      val b = Math.abs(r.nextInt(10*noHash))
      while( a % userToIndex.size == 0){
        a = Math.abs(r.nextInt(10*noHash))
      }
      hashA(h) = a
      hashB(h) = b
    }
    println("A values")
    hashA.foreach(x=>print(x+","))
    println(" ")
    println("B values")
    hashB.foreach(x=>print(x+","))
    println(" ")
    */

    // Create signature matrix
    val signature = Array.ofDim[Int](movieToIndex.size,noHash)

    // Initialize signature matrix with value infinity
    for(i <- 0 until noHash){
      for (j <- 0 until movieToIndex.size){
        // Assign infinite value
        signature(j)(i) = userToIndex.size*5
      }
    }

    // Update signature matrix one row in characteristic matrix at a time
    for(i <- 0 until userToIndex.size){

      // Generate all hash for row i
      val hashes = Array.fill[Int](noHash)(0)
      for(j <- 0 until noHash){
        hashes(j) = (hashA(j)*i + hashB(j)) % userToIndex.size
      }

      // Update signature matrix one column at a time
      for(j <- 0 until movieToIndex.size){

        // Only if characteristic matrix contains 1
        if (characteristic(j)(i) == 1){
          for (k <- 0 until noHash){
            if (signature(j)(k) > hashes(k)){
              signature(j)(k) = hashes(k)
            }
          }
        }
      }
    }

    //signature(0).foreach(x=>print(x+","))
    //println()
    //println(signature(0).length)
    //println(signature.length)

    // Select band value and compute row value and find candidates one band at a time
    var candidates = mutable.Map[(Int,Int),Double]()
    val r_value = 2
    val b_value = Math.round(noHash/r_value)

    // Go through signature matrix one band at a time in nested fashion
    for (i <- 0 until movieToIndex.size) {

      // Create index that tracks current band
      for (j <- i + 1 until movieToIndex.size) {

        // matchFlag is to break the loop once match is found
        var matchFlag = false
        var row_index = 0
        while( row_index < noHash - r_value){

          //println("Checking band : "+row_index/r_value)

          if (!matchFlag) {

            // Create sample vector from band length
            val s_left = Array.fill[Int](r_value)(0)
            val s_right = Array.fill[Int](r_value)(0)

            // Create partial signature S_j using only users' rating in current band
            for (k <- row_index until (row_index + r_value)) {
              if (k < noHash - 1) {
                s_left(k - row_index) = signature(i)(k)
                s_right(k - row_index) = signature(j)(k)
              }
            }
            row_index = row_index + r_value

            // Break the loop if these two are match after storing it in candidates pair
            if (s_left.mkString == s_right.mkString) {
              candidates += ((indexToMovie(i), indexToMovie(j)) -> 1.0)
              //println("added pair at index : "+i+" and : "+j)
              matchFlag = true
            }
          }
          else{
            row_index = noHash
          }
        }
      }
    }

    // Found all the candidates, compute jaccard, sort and dump to output file
    //candidates.foreach(println)
    println("Total candidates found : "+candidates.size)
    for( i <- candidates.keys ) {

      // Get left and right movieId index
      val left = characteristic(movieToIndex(i._1))
      val right = characteristic(movieToIndex(i._2))
      var union = 0.0
      var intersection = 0.0

      for (j <- left.indices){
        if ( left(j) == 0 && right(j) == 0 ){
          // Do nothing
        }
        else if (left(j) == right(j)){
          intersection = intersection + 1
          union = union + 1
        }
        else{
          union = union + 1
        }
      }

      // Compute jaccard and discard pairs below 0.5 jaccard distance
      val jaccard = intersection/union

      if (jaccard >= 0.5){
        candidates(i) = jaccard
      }
      else{
        candidates -= i
      }
    }
    //candidates.foreach(println)
    println("Total candidates after dropping candidates below 0.5 : "+candidates.size)

    // Dump to output file
    val writer = new FileWriter(new File(outfile))
    val sortedKeys = candidates.keySet.toList.sorted
    for (key <- sortedKeys){
      writer.write(key._1+","+key._2+","+candidates(key)+"\n")
    }
    writer.close()

    println("Total Hash functions : "+noHash)
    println("Total bands : "+b_value)

    // Calculate confusion matrix
    val groundtruthfile = "/home/mit2nil/INF553/HW4/data/SimilarMovies.GroundTruth.05.csv"
    if (new File(groundtruthfile).exists()){

      val groundtruth = sc.textFile(groundtruthfile).map(x=>((x.split(',')(0).toInt,x.split(',')(1).toInt),1)).collectAsMap()

      var tp = 0.0
      for(key <- candidates.keys){
        if (groundtruth.contains(key)){
          tp = tp + 1
        }
        else {
          println(key+" not found in ground truth")
        }
      }
      println("Precision is : "+Precision.round(tp/candidates.size,3))
      println("Recall is : "+Precision.round(tp/groundtruth.size,3))
    }

    // Compute S curve
    println("+++++++++++++")
    println(" S| (1-S^r)^b ")
    println("++++++++++++")
    val thresholds = List(0.2,0.3,0.4,0.5,0.6,0.7,0.8)
    for(s <- thresholds){
      println(s+"| "+(1-Math.pow(1-Math.pow(s,r_value),b_value)))
    }
    println("++++++++++++")
  }
}