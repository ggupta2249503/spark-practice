import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
object wordCount {
  def wordCountFunc(spark:SparkSession){
    
    //count of each words
    val df=spark.read.csv("src/main/resources/sample3.txt")
    println(df.rdd.getNumPartitions)
    //val df_rep=df.repartition(3)
    import spark.implicits._
    val df1=df.toDF("lines")
    val df2=df1.map(x=>{
      var lst=x.getString(0).split(" ")
      lst}).toDF("words")
      df2.show()
      val df3=df2.select(explode(col("words"))).toDF("words")
      val df4=df3.groupBy(col("words")).count()
      df4.show()
      
     //count of underscore
      val df_und=spark.read.csv("src/main/resources/sample4.txt")
    
    import spark.implicits._
    
    val df2_und=df_und.map(x=>{
      var lst=x.getString(0).split(" ")
      lst}).toDF("words")
      
     val df3_und=df2_und.select(explode(col("words"))).toDF("words")
     val df4_und=df3_und.filter(x=>x.getString(0).contains("_"))
     
     val count=df4_und.count()
     println(count)
    
    
    
    
  }
}