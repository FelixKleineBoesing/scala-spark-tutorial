package com.sparkTutorial.pairRdd.sort

import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCountProblem").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = sc.textFile("in/word_count.text")
    val splitted_words = words.
      flatMap(line => line.split(" ")).
      map(word => (word, 1))
    val word_counts = splitted_words.
      reduceByKey((x, y) => x + y).
      map(tup => (tup._2, tup._1)).
      sortByKey(ascending = false)
    for ((word, count) <- word_counts.collect()) {
      println("Word: " + word + ", Count: " + count)
    }
  }
}

