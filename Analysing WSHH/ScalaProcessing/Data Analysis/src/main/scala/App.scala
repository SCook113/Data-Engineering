/* App.scala */
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._


object App {
  def main(args: Array[String]) {
    
    // Set up BufferedWriter
    val file = new File("Data Reports For WSHH.txt")
    val writer = new BufferedWriter(new FileWriter(file))
  
    // Helper Function
    def writeToFile(text:String) : Unit = {
      val w = writer
      w.write(text)
      println(text)
      w.newLine();
    }
    
    // Helper Function
    def newLineToFile() : Unit = {
      val w = writer
      println("\n")
      w.newLine();
    }
    
    // Helper Function    
    def sectionSeperatorToFile() : Unit = {
      val w = writer
      writeToFile("######################################################################\n")
    }
    
    val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WWHH Statistics")
    val sc:SparkContext = new SparkContext(conf)
  
    //////////////////////////////////////////////////////
    // Let's bring the data into a form we can process 
    //////////////////////////////////////////////////////
    // Read the .csv file
    val input = sc.textFile("worldstarhiphop_posts_data_full.csv")
    
    //////////////////////////////////////////////////////
    // Format will be 
    // [ time_posted , views , title , time_crawled ]
    //////////////////////////////////////////////////////
    val formatted_rdd = input
    // Split String by ","
    .map(x => x.toString().split(","))
    // Place the columns back together 
    // Make sure the title is set back together right
    .map(y => ( y(1), y.last, y.drop(2).dropRight(1).toList.mkString(","), y(0)))
    // Remove the column titles and remove rows where there is no title for a post
    .filter(e => e._1 != "time_posted")
    // Remove lines with empty posts title
    .filter(e => e._3 != "")
    
    //////////////////////////////////////////////////////
    // How many videos have been posted?
    //////////////////////////////////////////////////////
    writeToFile("Sum of all posted videos on website: \n")
    val numPosts = formatted_rdd.count()
    writeToFile("There were " + numPosts + " Videos posted in total.")
    newLineToFile()
    sectionSeperatorToFile()
    
    //////////////////////////////////////////////////////
    // Views in total total per year:
    //////////////////////////////////////////////////////
    writeToFile("Views in total total per year: \n")
    val viewsPerYear = formatted_rdd
    // Map to [ year, views ]
    .map(x => ( x._1.split("-")(0) , BigInt(x._2)))
    .groupByKey()
    .map(e => (e._1 , e._2.reduceLeft(_ + _))).sortByKey().collect
    viewsPerYear.foreach(e => 
      writeToFile("In the Year " + e._1 + " the videos had a sum of " + e._2 + " views") )
    newLineToFile()
    newLineToFile()
    sectionSeperatorToFile()
    
    //////////////////////////////////////////////////////
    // Posts in total total per year:
    //////////////////////////////////////////////////////
    writeToFile("Posts in total total per year: \n")
    // How many videos are posted on average per year?
    val postsPerYear = formatted_rdd
    // Map to [ year, views ]
      .map(x => ( x._1.split("-")(0) , BigInt(x._2)))
      .groupByKey()
      .map(e => (e._1 , BigInt(e._2.size))).sortByKey().collect
      .foreach(e => 
      writeToFile("In the Year " + e._1 + " there were " + e._2 + " videos posted"))
    newLineToFile()
    newLineToFile()
    sectionSeperatorToFile()
    
    //////////////////////////////////////////////////////
    // Average number of views per video in a year:
    //////////////////////////////////////////////////////
    writeToFile("Average number of views per video in a year: \n")
    val averagePostsPerYear = formatted_rdd
      // Map to [ year, views ]
      .map(x => ( x._1.split("-")(0) , BigInt(x._2)))
      .groupByKey()
      .map(e => (e._1 , e._2.reduceLeft(_ + _) / BigInt(e._2.size))).sortByKey().collect
      .foreach(e => 
      writeToFile("In the Year " + e._1 + " a video had " + e._2 + " views on average"))
    newLineToFile()
    newLineToFile()
    sectionSeperatorToFile()
    
    //////////////////////////////////////////////////////
    // Most viewed 15 posts:
    //////////////////////////////////////////////////////
    writeToFile("The 15 most viewed posts: \n")
    formatted_rdd.
      map( e => ( BigInt(e._2) , e._3))
      .sortBy(_._1, ascending = false)
      .take(15)
      .foreach(e => writeToFile("Title: " + e._2 + ", views: " + e._1))
    newLineToFile()
    newLineToFile()
    sectionSeperatorToFile()

    //////////////////////////////////////////////////////
    // What are the top words used in the most viewed 10000 posts:
    // Here I did not filter stop words, but for word length of > 4 characters
    //////////////////////////////////////////////////////
    writeToFile("What are the top words used in the most viewed 10000 posts:\n")
    writeToFile("Here I did not filter out stop words, but filtered for word length of > 4 characters\n")
    formatted_rdd
      .map( e => ( BigInt(e._2) , e._3))
      .sortBy(_._1, ascending = false)
      .take(10000)
      .map( e => e._2.toString().split(" ") )
      .flatMap(x => x).filter(e => e.length() > 4)
      .toList.groupBy(x => x)
      .map( e => (e._1, e._2.toList.size)).toList
      .sortBy(_._2).reverse
      .take(40)
      .foreach(e => writeToFile(e.toString()))
    newLineToFile()
    newLineToFile()
    sectionSeperatorToFile()
    
    // Close BufferedWriter
    writer.close()
    }
}