package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    val conf = new SparkConf().setAppName("SumOfNumbers").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val prime_numbers = sc.textFile("in/prime_nums.text")
    var primes_flatten = prime_numbers.flatMap(line => line.split("\\s+"))
    primes_flatten = primes_flatten.filter(number => !number.isEmpty)
    val primes_flatten_number = primes_flatten.map(number => number.toInt)
    for (number <- primes_flatten_number) println("" + number)
    val sum_primes = primes_flatten_number.reduce((x, y) => x + y)

    println("Sum of primes: " + sum_primes)

  }
}
