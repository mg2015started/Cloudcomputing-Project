
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



/** Computes an approximation to pi */
object Lab_3 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Lab 3")
    val sc = new SparkContext(conf)
    val structFields = List(StructField("SL",DoubleType),StructField("LE",DoubleType),
      StructField("NP",DoubleType),StructField("AMH",DoubleType),StructField("TSC",DoubleType),
      StructField("WA",DoubleType),StructField("PL5",DoubleType),StructField("O",StringType),
      StructField("S",StringType),StructField("L",DoubleType))
    val structFields1=List(StructField("Dis",DoubleType))
    val types = StructType(structFields)
    val types1 = StructType(structFields1)
    val sqlContext = new SQLContext(sc)
    val rdd = sc.textFile("/home/mg2015started/cloudcomputing/HR_comma_sep.csv")
    val rowRdd = rdd.map(_.split(",")).map(line=>Row(line(0).toDouble,line(1).toDouble,
      line(2).toDouble,line(3).toDouble,line(4).toDouble,line(5).toDouble ,
      line(6).toDouble,line(7),line(8),line(9).toDouble))
    val df = sqlContext.createDataFrame(rowRdd,types)


    val rdd1=rdd.map(_.split(",")).map(line=>Row(Math.sqrt(Math.pow(line(0).toDouble-0.79,2)+Math.pow(line(1).toDouble-0.9,2))))
    val dfDis=sqlContext.createDataFrame(rdd1,types1)
    val df1=df.withColumn("Id",monotonically_increasing_id())
    val dfDis1=dfDis.withColumn("Id",monotonically_increasing_id())
    val dfResult=df1.join(dfDis1,"Id")

    val dfResult1=dfResult.sort(dfResult("Dis").asc).limit(5)
    val Left=dfResult1.filter("L==1")
    val notLeft=dfResult1.filter("L==0")
    println("The nearest 5 data sets are: ")
    dfResult1.show()
    if (Left.count()>notLeft.count())
      println("The candidate will leave")
    else
      println("The candidate will stay")
    sc.stop()




  }
}