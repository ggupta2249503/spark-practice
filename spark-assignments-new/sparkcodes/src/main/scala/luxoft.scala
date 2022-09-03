import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object luxoft {
  
  def luxsoft_function(spark:SparkSession){
    
    val arrayStructureData = Seq(
Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
)


 /* define the schema
create data frame
print the schema
dispaly the data
display the data where state=OH
write the same with sql expression
display the data where state=OH and gender=M
display the data where languages=java
 */

val rdd_array=spark.sparkContext.parallelize(arrayStructureData)
val schema=new StructType().add("name",new StructType().
    add("fname",StringType,true).add("mname",StringType,true).add("lname",StringType,true)).add("lang",ArrayType(StringType)).add("city",StringType).add("gender",StringType)
    
    val df_array=spark.createDataFrame(rdd_array,schema)
    df_array.printSchema()
    df_array.show()
    val df_array_exp=df_array.select(explode(col("lang")),col("name"),col("city"),col("gender"))
    val df_array_fil=df_array.filter(col("city")==="OH")
    
    df_array_fil.show(false)
    val df_array_fil_2=df_array.filter(col("city")==="OH" && col("gender")==="M")
    df_array_fil_2.show(false)
    df_array_exp.show(false)
    val df_array_exp_java=df_array_exp.filter(col("col")==="Java")
    df_array_exp_java.show(false)
  }
  
}