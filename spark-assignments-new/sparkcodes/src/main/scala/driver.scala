
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import publicis_sapient1.function1
import publicis_sapient2.function2
import luxoft.luxsoft_function
import impetus.impetus_function
import publicis_sapient3.windowFunctions
import cgi.cgiFunctions
import random_spark_ques.spark_random
import hcl.hclFunc
import wordCount.wordCountFunc
import questglobal.questFunc
object Test {
  def main(args:Array[String]){
    val spark=SparkSession.builder.appName("test").master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
      import spark.implicits._
      //diff kind of joins
      //val lst1 =List(1,1,1,1,2,2,3,5)
      //val lst2=List(1,1,2,2,3,4) 
      val lst1 =List(1,1,1,1)
      val lst2=List(1,1)
     
      val df1=lst1.toDF("id")
      val df2=lst2.toDF("id")
      val df_left=df1.join(df2,df1("id")===df2("id"),"leftouter")
      val df_right= df1.join(df2,df1("id")===df2("id"),"rightouter")
      val df_full= df1.join(df2,df1("id")===df2("id"),"fullouter")
      val df_inner= df1.join(df2,df1("id")===df2("id"))
      
      df_left.show(false)
      df_right.show(false)
      df_full.show(false)
      df_inner.show(false)
      
      val lst3=List(1,1,0,null,null)
     val lst3_row= lst3.map(x=>Row(x))
      val lst4=List(1,0,null)
      val lst4_row= lst4.map(x=>Row(x))
      val schema=StructType(Array(StructField("id",IntegerType,true)))
      val rdd3=spark.sparkContext.parallelize(lst3_row)
      val rdd4=spark.sparkContext.parallelize(lst4_row)
       val df3=spark.createDataFrame(rdd3,schema)
      val df4=spark.createDataFrame(rdd4,schema)
       val df_left1=df3.join(df4,df3("id")===df4("id"),"leftouter")
      val df_right1= df3.join(df4,df3("id")===df4("id"),"rightouter")
      val df_full1= df3.join(df4,df3("id")===df4("id"),"fullouter")
      val df_inner1= df3.join(df4,df3("id")===df4("id"))
      df_left1.show(false)
      df_right1.show(false)
      df_full1.show(false)
      df_inner1.show(false)
      
      
      
      //diff ways to create spark dataframe
      /*val lst=List((1,"Name:Prashant;salary:1000;role:DE"),(2,"Name:Shrishti;age:27;org:facebook;city:bangalore"))
    val lst_row= List(Row(1,"Name:Prashant;salary:1000;role:DE"),Row(2,"Name:Shrishti;age:27;org:facebook;city:bangalore"))
    val cols=List("id","value")
    val rdd=spark.sparkContext.parallelize(lst)
    val rdd_row=spark.sparkContext.parallelize(lst_row)
    val schema=StructType(Array(StructField("id_row",IntegerType,true),StructField("name_row",StringType,true)))
    val df_row=spark.createDataFrame(rdd_row,schema)
    df_row.show()
    val df_rdd=rdd.toDF
    df_rdd.show()
    val df_create=spark.createDataFrame(lst)
    df_create.show()
    val df=lst.toDF("id","values")*/
      //df_left.show()
      //df_right.show()
      //df_full.show()
      //df_inner.show()
      //function1(spark)
     //function2(spark)
        //luxsoft_function(spark)
        //impetus_function(spark)
      //windowFunctions(spark)
    //cgiFunctions(spark)
      //spark_random(spark)
      //hclFunc(spark)
      //wordCountFunc(spark)
      questFunc(spark)
 
   
   
   
   
    
   
  }
  
    
}