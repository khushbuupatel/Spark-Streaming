package BigDataAssignment2.A2T2;

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream.DStream

object Task2 {
  def main(args: Array[String]) {

    // exit the program if the path for the folder to monitor is not provided
    if (args.length < 4) {
      System.err.println("Provide input folder path as well as the output path for all three tasks!")
      System.exit(1)
    }

    // create the context
    val sparkConf = new SparkConf().setAppName("ScalaTasks").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val lines = ssc.textFileStream(args(0))

    // clean the words by removing the special characters
    val words_clean = lines.flatMap(_.trim().split("[,;:\\.'\"!\\?<>\\(\\)\\{\\}\\[\\]\\s+]"))
    val words_super_clean = words_clean.filter(word => !word.trim().isEmpty())

    // count the  word frequency for task 2.1
    val wordCounts = words_super_clean.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD(rdd => { if (!rdd.isEmpty()) { rdd.saveAsTextFile(args(1) + "/" + java.time.LocalDate.now + "-" + System.currentTimeMillis()) } })

    // filter short words for task 2.2
    val filtered_words = words_clean.filter(word => word.length() > 5)
    filtered_words.foreachRDD(rdd => { if (!rdd.isEmpty()) { rdd.saveAsTextFile(args(2) + "/" + java.time.LocalDate.now + "-" + System.currentTimeMillis()) } })

    // calculate co-occurrence matrix for task 2.3
    val clean_words = lines.map(_.trim().split("[,;:\\.'\"!\\?<>\\(\\)\\{\\}\\[\\]\\s+]").toList.filter(word => !word.trim().isEmpty()))
    val word_pair_frequency = clean_words.flatMap(_.combinations(2)).map((_, 1)).reduceByKey(_ + _)
    word_pair_frequency.foreachRDD(rdd => { if (!rdd.isEmpty()) { rdd.saveAsTextFile(args(3) + "/" + java.time.LocalDate.now + "-" + System.currentTimeMillis()) } })

    // stop the streaming only when the server is terminated
    ssc.start()
    ssc.awaitTermination()
  }
}