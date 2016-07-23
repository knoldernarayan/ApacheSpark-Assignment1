import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by narayan on 23/7/16.
  */
object PageCountApp extends App {

  val sparkConf = new SparkConf().setAppName("PageCountApp").setMaster("local[4]")
  val sparkContext = new SparkContext(sparkConf)

  //Q1. Create an RDD (Resilient Distributed Dataset) named ​pagecounts​ from the input file.

  val pagecounts: RDD[String] = sparkContext.textFile("/home/narayan/Projects/gargantua/pagecounts-20151201-220000")

  //Q2. Get the 10 records from the data and write the data which is getting printed/displayed.

  pagecounts.top(10).foreach(println)

  //Q3. How many records in total are in the given data set ?

  println(s"Number of records in given data set is ::::: ${pagecounts.count()} ")

  //Q4. Derive an RDD containing only English pages from ​pagecounts

  val enRdd:RDD[String] = pagecounts.filter(page => page.split(" ").apply(1).contains("en"))

  //Q5. How many records are there for English pages?

  println(s"There are :::: ${enRdd.count()} ::: records for English pages ")

  //Q6. ​Find the pages that were requested more than 200,000 times in total.

  val moreReqPage:RDD[String] = pagecounts.filter(page => page.split(" ").apply(2).toLong > 200000 )

  moreReqPage.top(10).foreach(println)

  println(s"There are :::: ${moreReqPage.count()} ::: pages that were requested more than 200,000 times ")
}
