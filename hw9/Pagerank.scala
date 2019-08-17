package lwx.hw7

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io._

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphLoader

object Pagerank 
{
  
  
    def pagerank():Unit=
    {
      val conf = new SparkConf().setAppName("Pagerank").setMaster("local[2]")
      val sc = new SparkContext(conf)
      // Load the edges as a graph
      val graph = GraphLoader.edgeListFile(sc, "followers.txt")
      // Run PageRank
      val ranks = graph.pageRank(0.0001).vertices
//      ranks.foreach(println)
      // Join the ranks with the usernames
      val users = sc.textFile("users.txt").map 
      { line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1))
      }
      var join_res_rdd=users.join(ranks)
//      join_res_rdd.foreach(println)
      val ranksByUsername = join_res_rdd.map 
      {
        case (id, (username, rank)) => (username, rank)
      }
      // Print the result
      println(ranksByUsername.collect().mkString("\n"))
    }
    
    
     def main(args: Array[String]) 
    {
        pagerank()
    }
}