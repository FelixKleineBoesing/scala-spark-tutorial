package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object UnionLogProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
       take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("unionLogs").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val log_first = sc.textFile("in/nasa_19950701.tsv")
    val log_second = sc.textFile("in/nasa_19950801.tsv")

    var aggregated_logs = log_first.union(log_second)

    aggregated_logs = aggregated_logs.filter(line => isNotHeader(line))

    val samples = aggregated_logs.sample(false, 0.1)

    samples.saveAsTextFile("out/sample_nasa_logs.tsv")
  }

  def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))
}
