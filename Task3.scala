import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Task3 {
  def main(args: Array[String]): Unit = {

    val input_path = args(0)
    val output_path = args(1)

    val conf = new SparkConf()
      .setAppName("Mahima_Task_3")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    val df1 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      //.load("/Users/mahima/IdeaProjects/DMAssignment1/src/main/scala/survey_results_public.csv")
      .load(input_path)
      .select("Country", "Salary", "SalaryType")


    val filteredDF = df1.filter(a => (a(1) != "NA") && (a(1) != "0"))

    val totalDF = filteredDF.select("Country", "Salary")
      .groupBy("Country")
      .count()
      .sort(asc("Country"))
      .as("tdf")


    val salariesDF = filteredDF.withColumn("Salary", when(col("SalaryType").equalTo("Weekly"),(col("Salary")*52).cast("Double")).otherwise(when(col("SalaryType").equalTo("Monthly"),(col("Salary")*12).cast("Double")).otherwise((col("Salary")*1)).cast("Double")))

    val avgDF = salariesDF.select("Country","Salary")
      .groupBy("Country")
      .agg(bround(avg("Salary"),2) as "Average_Salary")
      .sort(asc("Country"))
      .as("avgdf")


    val firstjDF = totalDF.join(avgDF, col("tdf.Country") === col("avgdf.Country"), "inner")
      .select("tdf.Country", "tdf.count", "avgdf.Average_Salary")
      .as("fjdf")

    val mmDF = salariesDF.select("Country","Salary")
      .groupBy("Country")
      .agg(min("Salary").cast(IntegerType) as "Minimum_Salary", max("Salary").cast(IntegerType) as "Maximum_Salary")
      .sort(asc("Country"))
      .as("mmdf")

    val secondjDF = mmDF.join(firstjDF, col("mmdf.Country") === col("fjdf.Country"), "inner")
      .select("mmdf.Country","fjdf.count","mmdf.Minimum_Salary","mmdf.Maximum_Salary","fjdf.Average_Salary")

    //secondjDF.coalesce(1).write.format("csv").save("/Users/mahima/IdeaProjects/DMAssignment1/src/main/scala/Mahima_Gupta_task3.csv")
    secondjDF.coalesce(1).write.format("csv").save(output_path)
  }
}
