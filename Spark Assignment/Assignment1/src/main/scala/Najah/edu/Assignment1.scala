package Najah.edu
import org.apache.spark.sql.SparkSession

object Assignment1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").appName("Assignment").getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")

    val textFiles = spark.sparkContext.wholeTextFiles("src/main/scala/Files/*")
    textFiles.collect().foreach(println);
    println("------------------------\n");
    val rddCount = {
      textFiles.flatMap(e => (e._2.split(" ").map(x =>(x,(1, List(e._1.split("/").last)))))).reduceByKey((x,y) => ((x._1 + y._1) , List.concat(x._2, y._2).distinct)).sortByKey();
    }

    /* val rddCount = {
        textFiles.flatMapValues(_.replaceAll("[\\n]"," ")).flatMap(e => (e._2.split(" ").map(x =>(x,(1, List(e._1.split("/").last)))))).reduceByKey((x,y) => ((x._1 + y._1) , List.concat(x._2, y._2).distinct)).sortByKey()
      }*/
    println("Sorted in alphapitcal order : ");
    val rddSorted = rddCount.map(a => (a._1, a._2)).sortByKey();
    rddSorted.foreach(println);
    println("-------------------------\n")
    // Sorted in new file .
    // rddSorted.coalesce(1).saveAsTextFile("TextWordCount");
     println("If you wont to search for one word please enter the nomber 1 Or enter number 2 if you wont to search for a sentence consisting of more than one word :  ")
    val number = scala.io.StdIn.readLine();
    if(number=="1"){
      println("Enter The word: ")
      val query = scala.io.StdIn.readLine();
      val foundName= rddSorted.filter(line=>line._1.contains(query))
      if(foundName.count()>0) {
        val listDoc = foundName.map(x => (x._2._2))
        listDoc.collect().foreach(println)
      }else{
        println("keyword not found in any document")
      }
    }
    if(number=="2"){
      println("Enter the sentence: ")
      val query2 = scala.io.StdIn.readLine()
      val rdd2= textFiles.map(e =>(e._2,e._1.split("/").last))
      val foundName2= rdd2.filter(line=>line._1.contains(query2))
      if(foundName2.count()>0) {
        val listDoc2 = foundName2.map(x => (x._2))
        listDoc2.collect().foreach(println)
      }else{
        println("sentence not found in any document")
      }
    }
    if(number != "1" && number !="2"){
      println("plese again and enter number 1 or 2")
    }
  }

}
