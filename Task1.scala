import java.io.{File, PrintWriter}
import org.apache.spark._

object Task1{
  def main(args: Array[String]): Unit = {

    val input_path = args(0)
    val output_path = args(1)

    val conf = new SparkConf()
      .setAppName("Mahima_Task_1")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    val rdd1 = spark
      .read
      .format("csv")
      .option("header", "true")
      //.load("/Users/mahima/IdeaProjects/DMAssignment1/src/main/scala/survey_results_public.csv")
      .load(input_path)
      .select("Country", "Salary", "SalaryType")
      .rdd


    val filterRDD = rdd1.filter(a => (a(1) != "NA") && (a(1) != "0"))


    val finalRDD = filterRDD.map(a => (a(0).toString,1))
      .reduceByKey(_+_)
      .sortByKey(true)
      .collect()

    //finalRDD.take(20).foreach(println)

    //val pwf = new PrintWriter(new File( "/Users/mahima/IdeaProjects/DMAssignment1/src/main/scala/Mahima_Gupta_task1.csv" ))
    val pwf = new PrintWriter(new File(output_path))
    pwf.write("Total" + ", " + filterRDD.count() + "\n")
    finalRDD.foreach( a => pwf.write( a._1 + ", " + a._2 + "\n" ))
    pwf.close()

  }
}