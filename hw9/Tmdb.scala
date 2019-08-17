package lwx.hw7

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io._
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex
import util.control.Breaks._
import scala.util._
import scala.collection.mutable.ArrayBuffer
//import spark.implicits._
 
object Tmdb 
{
//    def load_csv(_csv_path:String):Unit=
//    {
//        val spark = SparkSession.builder().appName("load_csv").master("local").getOrCreate()
//        val dataDF = spark.read.csv(_csv_path)
//        dataDF.show()
//    }
    
    def csv_filter(_prev_csv_path:String,_new_csv_path:String):Unit=
    {
      try
      {
          val prev_file=Source.fromFile(_prev_csv_path)
          val writer = new PrintWriter(new File(_new_csv_path))
          for(raw_line <- prev_file.getLines)
          {
            breakable
            {
                var pattern= "\".*\"".r
                var line_filtered=pattern replaceFirstIn(raw_line," ")
                pattern="\\[.*\\]".r
                line_filtered=pattern replaceFirstIn(raw_line," ")
                var items=line_filtered.split(',')
                var items_num=items.length

                if(items_num!=3)
                {
                  break()
                }
                
                var rate_col=items(2)
                var rate:Double=0
                var res=scala.util.Try(rate=rate_col.trim.toDouble)
//                println(res)
                var is_success=res match
                {
                  case Success(_)=>true
                  case _=>false
                }
                if(is_success==false)
                  break()
//                    println(rate)
                
                
                var revenue_col=items(1);

                pattern="[0-9]+".r
                var tmp_revenue:Double=0
                pattern.findFirstMatchIn(revenue_col) match
                {
                  case Some(a)=>
                    {
                      tmp_revenue=a.toString.toDouble
                    }
                  case _=>
                    {
                      tmp_revenue=0
                    }
                }         
//                    println(tmp_revenue)
                    
                
                //get names 
                pattern="\\[(.*)\\]".r
                var infos=pattern.findFirstMatchIn(raw_line) match
                {
                  case Some(a)=>a.group(1)
                  case _=>null
                }
                if(infos==null)
                  break()
//                println(infos)
                pattern="\\{(.*?)\\}".r
                var names=ArrayBuffer[String]() 
                for (patternMatch <- pattern.findAllMatchIn(infos))
                {
                  var tmp_info=patternMatch.group(1)
//                  println(tmp_info)
                  var tmp_items=tmp_info.split(',')
                  tmp_items=tmp_items(0).split(":")
                  var tmp_name=tmp_items(1)
                  pattern="\"{2}(.*)\"{2}".r
                  var name=pattern.findFirstMatchIn(tmp_name) match
                  {
                    case Some(a)=>a.group(1)
                    case _=>null
                  }
                  if(name==null)
                    break()
                  name=name.trim
                  names+=name
//                  println(name)
                }
                var names_array=names.toArray
                if(names_array.length==0)
                  break()
                for(name<-names_array)
                {
//                  println(name)
                    var line_new_buf=new StringBuilder
                    line_new_buf++=name+','
                    line_new_buf++=tmp_revenue.toString+','
                    line_new_buf++=rate.toString+"\n"
                    var line_new=line_new_buf.toString
//                    print(line_new)
                    writer.write(line_new)
                }
      
            }
              
          }
          prev_file.close()
          writer.close()
      }
      catch
      {
        case e:Exception => e.printStackTrace()
      }
    }
    
    case class Film(company_name:String,revenue:Double,rate:Double)
    
    def load_csv(_csv_path:String):Unit=
    {
        val spark = SparkSession.builder().appName("load_csv").master("local").getOrCreate()
        import spark.implicits._
//        val dataDF = spark.read.csv(_csv_path)
//        dataDF.withColumnRenamed("_c0","company_name")
//        dataDF.show()
         val filmRDD=spark.sparkContext.textFile(_csv_path)
         var filmDF=filmRDD.map(_.split(",")).map(a => Film(a(0), a(1).trim.toDouble,a(2).trim.toDouble)).toDF()
         filmDF.createOrReplaceTempView("film")
//         filmDF.show()
    }
    
    def tmdb_statistics(_res_path:String):Unit=
    {
        val spark = SparkSession.builder().appName("load_csv").master("local").getOrCreate()
        val resDF=spark.sql("SELECT company_name,SUM(revenue) FROM film WHERE rate>6.5 GROUP BY company_name")
        resDF.show()
//        res.write.format("csv").save("res.csv")
//        var dataFrameWithOnlyOneColumn = dataFrame.select(concat(res.columns).alias('data'))
//        dataFrameWithOnlyOneColumn.coalesce(1).write.format("text").option("header", "false").mode("append").save("<path>")
        var resRDD=resDF.rdd
        val writer = new PrintWriter(new File(_res_path))
        for(res_line<-resRDD.collect())
        {
//          println(res_line)
          writer.write(res_line.toString+"\n")
        }
        writer.close()
    }
  
    def main(args: Array[String]) 
    {
        val prev_csv_path="tmdb.csv"
        val new_csv_path="tmdb_new.csv"
//        csv_filter(prev_csv_path,new_csv_path)
        load_csv(new_csv_path)
        tmdb_statistics("tmdb_res.txt")
    }
    
    
}