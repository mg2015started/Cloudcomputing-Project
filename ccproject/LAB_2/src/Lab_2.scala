
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._



/** Computes an approximation to pi */
object Lab_2 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Lab 2")
    val sc = new SparkContext(conf)
    val structFields = List(StructField("SL",DoubleType),StructField("LE",DoubleType),
      StructField("NP",DoubleType),StructField("AMH",DoubleType),StructField("TSC",DoubleType),
      StructField("WA",DoubleType),StructField("PL5",DoubleType),StructField("O",StringType),
      StructField("S",StringType),StructField("L",DoubleType))
    val types = StructType(structFields)
    val sqlContext = new SQLContext(sc)
    val rdd = sc.textFile("/home/mg2015started/cloudcomputing/HR_comma_sep.csv")
    val rowRdd = rdd.map(_.split(",")).map(line=>Row(line(0).toDouble,line(1).toDouble,
      line(2).toDouble,line(3).toDouble,line(4).toDouble,line(5).toDouble ,
      line(6).toDouble,line(7),line(8),line(9).toDouble))
    val df = sqlContext.createDataFrame(rowRdd,types)
    val newIndex1 = new StringIndexer().setInputCol("S").setOutputCol("New S")
    val newIndex2 = new StringIndexer().setInputCol("O").setOutputCol("New O")
    val df1 = newIndex1.fit(df).transform(df)
    val df2 = newIndex2.fit(df1).transform(df1)
    df2.show()

    val correlation1=df2.stat.cov("LE","L")
    val correlation3=df2.stat.cov("AMH","L")
    val correlation4=df2.stat.cov("TSC","L")
    val correlation7=df2.stat.cov("New O","L")
    val correlation8=df2.stat.cov("New S","L")

   /*
       val input = sc.textFile("file:///home/hadoop/HR_comma_sep.csv")
    val data1=input.map(_.split(",")).map(p =>p(1).toDouble)
    val data3=input.map(_.split(",")).map(p =>p(3).toDouble)
    val data4=input.map(_.split(",")).map(p =>p(4).toDouble)
    val data7=input.map(_.split(",")).map(p =>p(7))
    val data8=input.map(_.split(",")).map(p =>p(8))
    val data9=input.map(_.split(",")).map(p =>p(9).toDouble)
    val correlation1: Double = Statistics.corr(data1, data9, "pearson")
    val correlation3: Double = Statistics.corr(data3, data9, "pearson")
    val correlation4: Double = Statistics.corr(data4, data9, "pearson")*/

    println("Last_Evalution: "+correlation1)
    println("Avg_Month_Hour: "+correlation3)
    println("Time_Spend_Company: "+correlation4)
    println("Occupation: "+correlation7)
    println("Salary: "+correlation8)
    //data.collect.foreach(println)

  }
}