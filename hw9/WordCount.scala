package lwx.hw7

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io._
 
object WordCount 
{
    def main(args: Array[String]) 
    {
        val prev_path="Shakespeare.txt"
        val new_path="Shakespeare_new.txt"
//        file_filter(prev_path,new_path)
        word_count(new_path)   
    }
    
    def file_filter(prev_path:String,new_path:String):Unit=
    {
        val prev_file=Source.fromFile(prev_path)
        val writer = new PrintWriter(new File(new_path))
        for(line <- prev_file.getLines)
        {
  //          val line_char_num=line.length();
            var new_line=new StringBuilder;
            for(c<-line)
            {
              var is_valid=(c>='a'&&c<='z')||(c>='A'&&c<='Z');
              var c_new =c
              if(!is_valid)
              {
                c_new=' ';
              }
              new_line+=c_new; 
            }
            var new_line_str=new_line.toString;
            writer.write(new_line_str+"\n")
        }
        prev_file.close
        writer.close();
    }
    
    def word_count(_file_path:String):Unit=
    {
//        val inputFile =  "file:///home/lwx/Documents/big_data/hw7/Shakespeare.txt"
        val inputFile =  _file_path
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val textFile = sc.textFile(inputFile)
        val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
//        wordCount.foreach(println)  
        val writer = new PrintWriter(new File("Shakespeare_wordcount.txt"))
        for(wordcount_pair<-wordCount.collect())
        {
          writer.write(wordcount_pair+"\n")
        }
        writer.close()
    }
}