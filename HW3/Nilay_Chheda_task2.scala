import java.io.{File, FileWriter}

import org.apache.commons.math3.util.Precision
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by mit2nil on 3/18/17.
  */
object hw3_task2 {
    def main(args: Array[String]) {

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
        val outfile = dirPath.concat("/Nilay_Chheda_result_task2.txt")

        // Load test data set
        val test_csv = sc.textFile(testfile)
        val test_header = test_csv.first()
        // pairRDD (movieID -> UserID)
        val rdd_test = test_csv.filter(x=>x!=test_header).map(x=>(x.split(',')(1).toInt,x.split(',')(0).toInt))
        val test = rdd_test.collect()

        //test.foreach(println)

        val data_csv = sc.textFile(datafile)
        val data_header = data_csv.first()
        // pairRDD (MovieID -> Map(UserID, Rating))
        val data = data_csv.filter(x=>x!=data_header).map(x=>(x.split(',')(1).toInt,Map(x.split(',')(0).toInt->x.split(',')(2).toDouble)))
            .reduceByKey((x,y)=>x++y).collect()

        // Create train set from data
        var train_buffer = ArrayBuffer.empty[(Int,Map[Int,Double])]
        for(i <- data.indices){
            val temp = rdd_test.lookup(data(i)._1)
            if (temp.nonEmpty){
                var userIDs = data(i)._2
                if (userIDs.keys.toArray.contains(temp.head)){
                    //println("MovieID : "+data(i)._1+" Before : "+userIDs)
                    userIDs -= temp.head
                    if (userIDs.nonEmpty){
                        train_buffer += ((data(i)._1, userIDs))
                    }
                    //println("MovieID : "+data(i)._1+" After : "+userIDs)
                }
                else{
                    train_buffer += ((data(i)._1, data(i)._2))
                }
            }
            else{
                train_buffer += ((data(i)._1, data(i)._2))
            }
        }
        val train = sc.parallelize(train_buffer.map(x=>(x._1,x._2))).reduceByKey((x,y)=>x++y).collect()

        //data.foreach(println)
        //train.foreach(println)

        // RDD (UserID, MovieID => rating)
        var predictions = Map[(Int,Int),Double]()
        var missing_predictions = Map[(Int,Int),Double]()

        // for every movie(m)-user(u) pair in test set
        for(i <- test.indices){

            // Only consider map with matching movie id
            val movie_i = test(i)._1

            // Ignore user rating with test user as we need to predict it
            val user_v = test(i)._2

            // Find movie i record
            var r_i_bar = 0.00
            // MovieId -> Rating for user u
            var r_ui = Map[Int,Double]()
            var count = 0

            // calculate average rating of movie i
            for(j <- train.indices){

                // calculate average rating <r_i>, loop will run only in one case
                if (movie_i == train(j)._1){

                    // Go over all users who rated movie i
                    val userRatings = train(j)._2

                    // Calculate average rating over only co rated movies
                    if (userRatings.contains(user_v)){
                        for(k <- userRatings.keys){
                            if (user_v != k){
                                r_i_bar = r_i_bar + userRatings(k)
                                r_ui += (k->userRatings(k))
                                count = count + 1
                            }
                        }
                    }
                }
            }
            // find average
            r_i_bar = r_i_bar/count

            //println("movie_i : "+movie_i+" user_u : "+user_v+" r_i_bar : "+r_i_bar+"\n r_u_i : "+r_ui)

            // Do one more pass through all movies and calculate all w_ij
            // Map from MovieID_j -> W_ij between movie_i and movie_j
            var w_ijs = Map[Int,Double]()
            for(j <- train.indices) {

                // Movie j
                //val movie_j = train(j)._1

                // User ratings for movie j
                val userRatings = train(j)._2

                // Ensure that user v has co rated MovieID j
                if (userRatings.contains(user_v)){

                    // pre compute average rating of movie r_j_bar
                    var r_j_bar = 0.00
                    count = 0
                    for(k <- userRatings.keys){
                        // only consider ratings from users other than v
                        if (user_v != k){
                            r_j_bar = r_j_bar + userRatings(k)
                            count = count + 1
                        }
                    }
                    r_j_bar = r_j_bar/count

                    // Loop over all users to calculate w_ij
                    var w_ij_numerator = 0.00
                    var w_ij_denominator_1 = 0.00
                    var w_ij_denominator_2 = 0.00
                    count = 0
                    for(k <- userRatings.keys){

                        // Move ahead only if user has rated both movie i and j
                        if (r_ui.contains(k)){
                            w_ij_numerator = w_ij_numerator + (r_ui(k) - r_i_bar)*(userRatings(k)-r_j_bar)
                            w_ij_denominator_1 = w_ij_denominator_1 + (r_ui(k) - r_i_bar)*(r_ui(k) - r_i_bar)
                            w_ij_denominator_2 = w_ij_denominator_2 + (userRatings(k)-r_j_bar)*(userRatings(k)-r_j_bar)
                            count = count + 1
                        }
                    }

                    // Scale back weight based on number of users who corated this movie
                    if (count > 50){
                        val temp = Math.sqrt(w_ij_denominator_1)*Math.sqrt(w_ij_denominator_2)
                        w_ijs += j -> Precision.round(w_ij_numerator/temp, 1)
                    }
                    else if (count > 40){
                        val temp = Math.sqrt(w_ij_denominator_1)*Math.sqrt(w_ij_denominator_2)
                        w_ijs += j -> Precision.round(0.8*w_ij_numerator/temp, 1)
                    }
                    else if (count > 30){
                        val temp = Math.sqrt(w_ij_denominator_1)*Math.sqrt(w_ij_denominator_2)
                        w_ijs += j -> Precision.round(0.6*w_ij_numerator/temp, 1)
                    }
                    else if (count > 20){
                        val temp = Math.sqrt(w_ij_denominator_1)*Math.sqrt(w_ij_denominator_2)
                        w_ijs += j -> Precision.round(0.4*w_ij_numerator/temp, 1)
                    }
                    else if (count > 10){
                        val temp = Math.sqrt(w_ij_denominator_1)*Math.sqrt(w_ij_denominator_2)
                        w_ijs += j -> Precision.round(0.2*w_ij_numerator/temp, 1)
                    }
                    else {
                        val temp = Math.sqrt(w_ij_denominator_1)*Math.sqrt(w_ij_denominator_2)
                        w_ijs += j -> Precision.round(0.1*w_ij_numerator/temp, 1)
                    }
                }
            }

            // Store k nearest neighbour sorted by weight w_ij
            // NN => RDD(MovieID -> W_ij)
            val nn = w_ijs.toArray.sortBy{case(x,y)=>(x,-y)}

            // Run over top K nearest neighbour and calculate prediction
            var pred_numerator = 0.00
            var pred_denominator = 0.00
            count = 0
            for(j <- nn.indices){
                // Threshold for selecting neighbourhood
                if (nn(j)._2 > 0.5) {
                    if (count < 5) {

                        // Find UserID->Rating for MovieID j of W_ij
                        var rating_ui = 0.0
                        for (k <- train.indices) {
                            // Capture user V rating on jth movieID from w_ij
                            if (train(k)._1 == nn(j)._1) {
                                //println("For MovieID : "+nn(j)._1+" Retrieving User->Rating: "+train(nn(j)._1)._2)
                                rating_ui = train(nn(j)._1)._2(user_v)
                            }
                        }

                        pred_numerator = pred_numerator + rating_ui * nn(j)._2
                        pred_denominator = pred_denominator + Math.abs(nn(j)._2)
                    }
                    count = count + 1
                }
            }

            if (pred_denominator == 0  || count < 4){
                // Add test case to missing predictions if denominator is zero
                missing_predictions += (user_v,movie_i) -> 0.0
            }
            else{
                // Calculate prediction based on item based CF formula
                predictions += (user_v,movie_i) -> Precision.round(pred_numerator/pred_denominator,1)
            }
        }
        //predictions.foreach(println)

        // Root mean square calculation between trained
        var pred = sc.parallelize(predictions.keys.map(x=>(x,predictions(x))).toSeq)

        // Handle cases where prediction wasn't good

        // calculate average rating for users in missing predictions
        val userAvgRatings = data_csv.filter(x=>x!=data_header).map(x=>(x.split(',')(0).toInt,x.split(',')(2).toDouble))
            .mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
            .map{case (k, v) => (k, Precision.round(v._1 / v._2.toFloat,1))}.collectAsMap()

        val pred2 = sc.parallelize(missing_predictions.map(x=>(x._1,userAvgRatings(x._1._1))).toSeq)
        pred = pred.union(pred2)

        val err = data_csv.filter(x=>x!=data_header).map(x=>((x.split(',')(0).toInt,x.split(',')(1).toInt), x.split(',')(2).toDouble))
                .join(pred).map { case ((user, product), (r1, r2)) => Math.abs(r1-r2)}
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
        val predictions_formatted = pred.collect().sorted
        val writer = new FileWriter(new File(outfile))
        writer.write("UserId,MovieId,Pred_rating\n")

        predictions_formatted.foreach(x=>{
            writer.write(x._1._1+","+x._1._2+","+x._2+"\n")
        })
        writer.close()
    }
}

