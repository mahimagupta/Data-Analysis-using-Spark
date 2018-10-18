import java.io.{File, PrintWriter}
import org.apache.spark.{SparkContext,SparkConf,HashPartitioner}

object Task2{
  def main(args: Array[String]): Unit = {

    val input_path = args(0)
    val output_path = args(1)

    val conf = new SparkConf()
      .setAppName("Mahima_Task_2")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    val rdd1 = spark
      .read
      .format("csv")
      .option("header", "true") //reading the headers
      //.load("/Users/mahima/IdeaProjects/DMAssignment1/src/main/scala/survey_results_public.csv")
      .load(input_path)
      .select("Country", "Salary", "SalaryType")
      .rdd


    val filterRDD = rdd1.filter(a => (a(1) != "NA") && (a(1) != "0"))
    val map1 = filterRDD.map(a => (a(0).toString,1))
    val sizeofmap1 = map1.mapPartitions(iter => Array(iter.size).iterator, true).collect()
    //sizeofmap1.foreach(println)

    val map2=filterRDD.map(a => (a(0).toString,1)).partitionBy(new HashPartitioner(2))
    val sizeofmap2 = map2.mapPartitions(iter => Array(iter.size).iterator, true).collect()
    //sizeofmap2.foreach(println)


    val time1=System.currentTimeMillis()

    val reduce1= map1
      .reduceByKey(_+_)
      .sortByKey(true)
    val duration1 = (System.currentTimeMillis() - time1).toInt
    //print(duration1)

    val time2= System.currentTimeMillis()

    val reduce2= map2
      .reduceByKey(_+_)
      .sortByKey(true)
    val duration2= (System.currentTimeMillis() - time2).toInt
    //print(duration2)

   //val pwf = new PrintWriter(new File( "/Users/mahima/IdeaProjects/DMAssignment1/src/main/scala/Mahima_Gupta_task2.csv" ))
   val pwf = new PrintWriter(new File(output_path))
    pwf.write("Standard" + ", " + sizeofmap1(0) + ", " + sizeofmap1(1)  + ", " + duration1 + "\n" )
    pwf.write("Partition" + "," + sizeofmap2(0) + ", " + sizeofmap2(1)  + ", " + duration2 + "\n")
    pwf.close()

  }
}

