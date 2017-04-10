/**
  * Created by mit2nil on 3/8/17.
  */

import java.io.{File, FileWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object hw3_task1 {

    def main(args: Array[String]){

        // Turn off logs
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        // Spark context
        val sparkConf = new SparkConf().setAppName("inf553").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)

        // Output file logic
        val datafile = args(0)
        val testfile = args(1)
        val f = new File(testfile)
        val dirPath = f.getAbsoluteFile.getParentFile.getAbsolutePath
        var outfile = ""

        // Configure output file dir
        if (testfile.contains("small")) {
            outfile = dirPath.concat("/Nilay_Chheda_result_task1_small.txt")
        }
        else{
            outfile = dirPath.concat("/Nilay_Chheda_result_task1_big.txt")
        }

        // Load test data set
        val test_csv = sc.textFile(testfile)
        val test_header = test_csv.first()
        val test_x = test_csv.filter(x=>x!=test_header).map(x=>((x.split(',')(0).toInt,x.split(',')(1).toInt), 0.0))

        // Load train data set
        val data_csv = sc.textFile(datafile)
        val data_header = data_csv.first()
        val data = data_csv.filter(x=>x!=data_header).map(x=>((x.split(',')(0).toInt,x.split(',')(1).toInt), x.split(',')(2).toDouble))

        val train = data.subtractByKey(test_x)
        val test = data.subtractByKey(train)

        // Ratings from the formatted train data
        val ratings = train.map(x => Rating(x._1._1, x._1._2, x._2))

        // Build the recommendation model using ALS
        val rank = 5
        val numIterations = 10
        val lambda = 0.1
        val model = ALS.train(ratings, rank, numIterations, lambda)

        // Apply the model on test data set and join it existing test data set for RMSE calculations
        //println("train + test = d : "+train.count()+" + "+test.keys.count()+" = "+data.count())
        val model_predictions = model.predict(test.keys).map { case Rating(user, product, rate) => ((user, product), rate)}
        //println("Model predicted - "+model_predictions.count())

        // calculate average rating for users in missing predictions
        val userAvgRatings = train.map(x => (x._1._1,x._2)).mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
            .map{case (k, v) => (k, v._1 / v._2.toFloat)}.collectAsMap()

        val missing_predictions = test_x.subtractByKey(model_predictions).map{case(k,v)=>(k,userAvgRatings(k._1))}
        //println("Missing predictions are - "+missing_predictions.count())

        // Join model based and manual predictions
        val predictions = model_predictions.union(missing_predictions)
        //println("Total predictions are - "+predictions.count())

        // Root mean square calculation between trained
        val err = test.join(predictions).map { case ((user, product), (r1, r2)) => Math.abs(r1-r2)}
        val RMSE = Math.sqrt(err.map(x=>{x*x}).mean())

        val errorVals = err.collect()
        val count = Array[Int](0,0,0,0,0)
        for(i <- errorVals.indices){
            val e = errorVals(i)
            if (e < 1){
                count(0) += 1
            }
            else if (e < 2){
                count(1) += 1
            }
            else if (e < 3){
                count(2) += 1
            }
            else if (e < 4){
                count(3) += 1
            }
            else{
                count(4) += 1
            }
        }

        // Print to command line
        println(">=0 and <1: "+count(0))
        println(">=1 and <2: "+count(1))
        println(">=2 and <3: "+count(2))
        println(">=3 and <4: "+count(3))
        println(">=4 : "+count(4))
        println("RMSE = " + RMSE)

        // Write to file
        val predictions_formatted = predictions.collect().sorted
        val writer = new FileWriter(new File(outfile))
        writer.write("UserId,MovieId,Pred_rating\n")
        
        predictions_formatted.foreach(x=>{
            writer.write(x._1._1+","+x._1._2+","+x._2+"\n")
        })
        writer.close()
    }
}
