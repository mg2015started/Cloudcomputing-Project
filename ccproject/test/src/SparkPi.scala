
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Pi")
    val sc = new SparkContext(conf)
    /*val input = sc.textFile("file:///home/hadoop/HR_comma_sep.csv")
    input.collect().foreach(println)
    val result = input.map{ line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext()
    }
    println(result.getClass)
    result.collect().foreach(x => {x.foreach(println);println("======")})*/
    val input = sc.textFile("home/mg2015started/cloudcomputing/HR_comma_sep.csv")
    val data = input.map(x =>
      (x.split(",")(0),x.split(",")(1),x.split(",")(2),
        x.split(",")(3),x.split(",")(4),x.split(",")(5),
        x.split(",")(6), x.split(",")(7),x.split(",")(8),
        x.split(",")(9)))
    //data.collect.foreach(println)
    var sort1=data.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x0.toDouble>0&&x0.toDouble<=0.2}
    var sort1_1=sort1.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x9.toInt==1}
    var tmp1=(sort1_1.count().toDouble/sort1.count.toDouble)*100
    var result1=tmp1.formatted("%.4f")

    var sort2=data.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x0.toDouble>0.2&&x0.toDouble<=0.4}
    var sort2_1=sort1.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x9.toInt==1}
    var tmp2=(sort2_1.count().toDouble/sort2.count.toDouble)*100
    var result2=tmp2.formatted("%.4f")

    var sort3=data.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x0.toDouble>0.4&&x0.toDouble<=0.6}
    var sort3_1=sort1.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x9.toInt==1}
    var tmp3=(sort3_1.count().toDouble/sort3.count.toDouble)*100
    var result3=tmp3.formatted("%.4f")

    var sort4=data.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x0.toDouble>0.6&&x0.toDouble<=0.8}
    var sort4_1=sort1.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x9.toInt==1}
    var tmp4=(sort4_1.count().toDouble/sort4.count.toDouble)*100
    var result4=tmp4.formatted("%.4f")

    var sort5=data.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x0.toDouble>0.8&&x0.toDouble<=1.0}
    var sort5_1=sort1.filter{case(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9) => x9.toInt==1}
    var tmp5=(sort5_1.count().toDouble/sort5.count.toDouble)*100
    var result5=tmp5.formatted("%.4f")
    println("(0.0,0.2] Left Percentage: "+result1+"%")
    println("(0.2,0.4] Left Percentage: "+result2+"%")
    println("(0.4,0.6] Left Percentage: "+result3+"%")
    println("(0.6,0.8] Left Percentage: "+result4+"%")
    println("(0.8,1.0] Left Percentage: "+result5+"%")


   /* val reader = new CSVReader(new FileReader("/home/hadoop/HR_comma_sep.csv"))
    for (row <- reader.readAll) {
      println("In " + row(0) + " there were " + row(1) + " unruly passengers.")
    }*/

   /* var conf=new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / (n - 1))
    spark.stop()*/
  }
}