package com.sparkTutorial.rdd.nasaApacheWebLogs

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    val conf = new SparkConf().setAppName("SameHosts").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val july_logs = sc.textFile("in/nasa_19950701.tsv")
    val august_logs = sc.textFile("in/nasa_19950801.tsv")

    val july_hosts = july_logs.map(line => line.split("\t")(0))
    val august_hosts = august_logs.map(line => line.split("\t")(0))

    val same_hosts = july_hosts.intersection(august_hosts).filter(host => host != "host")
    same_hosts.saveAsTextFile("out/nasa_logs_same_hosts.csv")



  }
}
