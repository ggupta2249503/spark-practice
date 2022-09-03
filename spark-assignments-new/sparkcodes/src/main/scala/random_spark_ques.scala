import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object random_spark_ques {
  
  def spark_random(spark:SparkSession){
    
    val list=List(("X", 5, 3, 7),("Y", 3, 3, 6),("Z", 5, 2, 6))
    import spark.implicits._
    val df=list.toDF("A","B","C","D")
    val df_new=df.withColumn("newcol", col("B")+col("C")+col("D"))
    df_new.show()
    
    def sum_cols(x:Int,y:Int,z:Int):Int = {
      
      x+y+z
    }
    
    val sum_inline=(x:Int,y:Int,z:Int)=>x+y+z
    val sum_udf=udf(sum_cols(_,_,_))
    val df_with_udf=df.withColumn("newcol_udf",sum_udf(col("B"),col("C"),col("D")))
    df_with_udf.show()
    
    val df_with_udf_inline_function=df.withColumn("newcol_udf_inline",sum_udf(col("B"),col("C"),col("D")))
    df_with_udf_inline_function.show()
    
   val lst=List((1,"Name:Prashant;salary:1000;role:DE"),(2,"Name:Shrishti;age:27;org:facebook;city:bangalore"))
   import spark.implicits._
   val df_name=lst.toDF("id","values")
   val maps=scala.collection.mutable.Map[String,String]()
   val df_name1=df_name.map{x=>
     var lst_semi=x.getString(1).split(";").map(x=>x.split(":")).foreach(x=> maps+=(x(0)->x(1)))
     //var lst_colon=lst_semi.map(x=>x.split(":"))
     //lst_colon.map(x=>maps+=(x(0)->x(1))
     //lst_colon.foreach(x=> maps+=(x(0)->x(1)))
         (x.getInt(0),maps)
         }.toDF("id","values")
         
      val df_name1_exp=df_name1.select(col("id"),explode(col("values")))
   df_name1_exp.show(false)
   
   val lst_user=List(("user1","Mayank Codes in #Spark and #Java"),("user2","Coding in #Spark #Python is easy"),("user1","Priyank codes in #Python #Java"),("user2","Coding in #Spark #Scala is super easy"))
   val lst_user_df=lst_user.toDF("name","values")
   val lst_user_df1=lst_user_df.map(x=>(x.getString(0),x.getString(1).split(" "))).toDF("name","values")
   val lst_user_df2=lst_user_df1.select(col("name"),explode(col("values"))).toDF("name","values").filter(col("values").startsWith("#"))
   val lst_user_df3=lst_user_df2.groupBy(col("name")).agg(collect_set(col("values")))
   
   lst_user_df3.show()
   
   
   //creating an empty dataframe
   val schema=StructType(Array(StructField("id",StringType,true)))
   val rdd_empty=spark.sparkContext.emptyRDD[Row]
   val df_empty=spark.createDataFrame(rdd_empty, schema)
   val boo=df_empty.isEmpty
   println(boo)
   val lst_null=List(1,2,null,3,null)
   val lst_row_null=lst_null.map(x=>Row(x))
   val rdd_row_null=spark.sparkContext.parallelize(lst_row_null)
   val schema_null=StructType(Array(StructField("id",IntegerType,true)))
   val df_null=spark.createDataFrame(rdd_row_null,schema_null)
   val df_without_null=df_null.filter(col("id").isNotNull).count()
   println(df_without_null)
   
   
   
    
    
    
  }
}