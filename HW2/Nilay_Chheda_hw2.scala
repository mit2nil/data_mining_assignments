import java.io.{File, FileWriter}

import org.apache.spark.storage.StorageLevel

import scala.math.Ordering.Implicits._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by mit2nil on 2/25/17.
  */

object Nilay_Chheda_hw2 {

    def dumpOutput(o: Array[Set[Int]], k: Int, f: String) : Unit = {

        // Sort everything
        var r = ArrayBuffer.empty[List[Int]]
        for(i<- o.indices){
            r += o(i).toList.sorted
        }

        r = r.sorted

        val writer = new FileWriter(new File(f),k!=1)
        //print("(" + r(0)(0))
        writer.write("(" + r(0).head)
        for (i <- 1 until k) {
            //print(", " + r(0)(i))
            writer.write(", " + r(0)(i))
        }
        //print(")")
        writer.write(")")

        for (j <- 1 until r.length) {
            //print(", (" + r(j)(0))
            writer.write(", (" + r(j).head)
            for (i <- 1 until k) {
                //print(", " + r(j)(i))
                writer.write(", " + r(j)(i))
            }
            //print(")")
            writer.write(")")
        }
        //println()
        writer.write("\n")
        writer.close()
    }

    def main(args: Array[String]) {

        // Setup variables
        val start = System.currentTimeMillis()
        var end = System.currentTimeMillis()
        val caseN = args(0).toInt
        val infile = args(1)
        val support = args(2).toInt
        val f = new File(infile)
        val dirPath = f.getAbsoluteFile.getParentFile.getAbsolutePath
        var outfile = ""

        // Configure output file dir
        if (infile.contains("movies.small")) {
            outfile = dirPath.concat("/Nilay_Chheda_SON_Small2.case" + caseN + ".txt")
        }
        else if (infile.contains("ml-latest")) {
            outfile = dirPath.concat("/Nilay_Chheda_SON_MovieLens.Small.Case" + caseN + "-" + support + ".txt")
        }
        else if (infile.contains("ml-20m")) {
            outfile = dirPath.concat("/Nilay_Chheda_SON_MovieLens.Big.Case" + caseN + "-" + support + ".txt")
        }

        // Configure Spark context
        val sparkConf = new SparkConf().setAppName("inf553").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)

        // Read data file and remove csv header
        var csv = sc.textFile(infile)
        val header = csv.first()
        var baskets = sc.parallelize(List(0).map(_=>(0,Set(0))))
        var total_cnt = 0
        csv = csv.filter(line => line != header)

        // Create baskets { userId -> MovieIDs } or { userId <- MovieIDs }
        if (caseN == 1) {

            // Read all baskets and store total basket count
            baskets = csv.map(line => (line.split(",")(0).toInt, Set(line.split(",")(1).toInt)))
                .reduceByKey((x,y)=>x.union(y))
            //baskets.foreach(println)
        }
        else{
            // Read all baskets and store total basket count
            baskets = csv.map(line => (line.split(",")(1).toInt, Set(line.split(",")(0).toInt)))
                .reduceByKey((x,y)=>x.union(y))
            //baskets.foreach(println)
        }

        total_cnt = baskets.count().toInt

        // Run SON algorithm on item sets of size k = 1, 2, ... until no new item sets

        var candidates_count = 1
        var k = 1
        var item_sets = Array.empty[Set[Int]]
        while (candidates_count > 0) {

            //println("Case K = " + k)

            if (k == 1) {
                // One line logic for singletons
                item_sets = baskets.flatMapValues(x => x.subsets(1)).map{case(_,y)=>(y,1)}.reduceByKey(_+_).filter(x=>x._2>=support).keys.collect()
                candidates_count = item_sets.length
                dumpOutput(item_sets, k, outfile)
                //item_sets.foreach(println)
            }
            else {
                // ======================================
                // Step 1 - Find candidates from k-1 case
                // ======================================
                var m = mutable.Map[Set[Int], Int]()

                // n^2 loop to find k+1 size item_sets
                for (i <- item_sets.indices) {
                    for (j <- i + 1 until item_sets.length) {
                        val s = item_sets(i).union(item_sets(j))
                        //println(s)
                        //println(s.size+"-"+(item_sets(i).size+1))
                        if (s.size == (item_sets(i).size + 1)) {
                            m += (s -> (m.getOrElse(s, 0) + 1))
                        }
                    }
                }

                //println(m)

                // (n choose n-1 ) choose 2 = n choose 2 = n*(n-1)/2
                // For (1,2,3,4) => (1,2,3)U(2,3,4), (1,2,4)U(1,2,3), (1,3,4)U(2,3,4) => 3 union
                // For (1,2,3) => (1,2)U(2,3), (1,2)U(1,3), (1,3)U(2,3) => 3 union
                // For (1,2) => (1)U(2) => 1 union
                for (c <- m.keys) {
                    val cnt = c.size*(c.size-1)/2
                    if (m(c) != cnt) {
                        m -= c
                    }
                }

                //println(m)

                // ======================================
                // Step 2 - Find candidates from chunks
                // ======================================

                // Filter candidates that are less than ps in respective chunks
                val candidates = baskets.mapPartitions { chunk =>
                    val l = chunk.toList
                    val ps = (l.size * support) / total_cnt
                    var c = mutable.Map[Set[Int], Int]()

                    // Count item set in every chunk and filer based on ps
                    m.keys.foreach(x => {
                        val item_set = x
                        //println(" checking candidate "+item_set)
                        // Check all baskets
                        for (basket <- l) {
                            val items = basket._2
                            //println(" in "+items)
                            if (item_set.subsetOf(items)) {
                                //println("++1")
                                c += (item_set -> (c.getOrElse(item_set, 0) + 1))
                            }
                        }

                        // Further filtering
                        if (c.contains(item_set) &&  c(item_set) < Math.floor(ps)) {
                            //println("Removing item set : "+item_set)
                            c -= item_set
                        }
                    })
                    //l.foreach(println)
                    //println(c)
                    c.iterator
                }.reduceByKey((_,_)=>0).keys.collect()

                // ======================================
                // Step 3 - Find finals set of candidates
                // ======================================

                val temp = baskets.mapPartitions { chunk =>
                    val l = chunk.toList
                    var c = mutable.Map[Set[Int], Int]()
                        candidates.foreach(item_set => {
                        //println(" checking candidate "+item_set)
                        // Check all baskets
                        for (basket <- l) {
                            val items = basket._2
                            //println(" in "+items)
                            if (item_set.subsetOf(items)) {
                                //println("++1")
                                c += (item_set -> (c.getOrElse(item_set, 0) + 1))
                            }
                        }
                    })
                    //l.foreach(println)
                    //println(c)
                    c.iterator
                }.reduceByKey(_+_).filter(x=>x._2>=support).keys.collect()

                candidates_count = temp.length
                if (candidates_count > 0) {
                    item_sets = temp
                    dumpOutput(item_sets, k, outfile)
                }
            }
            k = k + 1
        }
        end = System.currentTimeMillis()
        println("Took : " + ((end - start) / 1000))
    }
}