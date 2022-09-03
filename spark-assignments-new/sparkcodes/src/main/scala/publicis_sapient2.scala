
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object publicis_sapient2 {
  def function2(spark:SparkSession){
    
    /*
user1| Mayank Codes in #Spark and #Java
user2| Coding in #Spark #Python is easy
user1| Priyank codes in #Python #Java
user2| Coding in #Spark #Scala is super easy

user1|[#Spark, #Java, #Python]
user2|[#Python, #Spark, #Scala]*/
    
import spark.implicits._
val lst_user=List(("user1","Mayank Codes in #Spark and #Java"),("user2","Coding in #Spark #Python is easy"),("user1","Priyank codes in #Python #Java"),("user2","Coding in #Spark #Scala is super easy"))
val rdd_user=spark.sparkContext.parallelize(lst_user)
val cols_user=List("name","lang")
//val df_user=spark.createDataFrame(rdd_user,cols_user)
val df_user=rdd_user.toDF("name","lang")
   val df_user_cnt=df_user.map(x=>{
     var lst_cnt=scala.collection.mutable.ListBuffer[String]()
     var lst=x.getString(1).split(" ")
     lst.map(x=>{
       if(x.startsWith("#")){
         lst_cnt.append(x)
       }
       }
   )
   (x.getString(0),lst_cnt)
   
     
     
   })
   df_user_cnt.show(false)
   val df_user1=df_user.map(x=>(x.getString(0),x.getString(1).split(" "))).toDF("name","lang")
   val df_user2=df_user1.select($"name",explode($"lang"))
   df_user2.show(false)
   val df_user2_fil=df_user2.filter(x=>x.getString(1).startsWith("#"))
   
   //val df_user2_fil_grp=df_user2_fil.groupBy("name","col").count()
   df_user2_fil.createOrReplaceTempView("table1")
   val df_user2_fil_part=spark.sqlContext.sql("select name,col,dense_rank() over(partition by name order by col) as rn from table1")
   df_user2_fil.show(false)
   df_user2_fil_part.show(false)
  val df_user2_fil_grp1=df_user2_fil.dropDuplicates("name", "col")
  df_user2_fil_grp1.show(false)

   //val lst_coll=df_user2_fil.collect()
   //val lstBuffUser=scala.collection.mutable.Set[String]()
   
   //df_user2_fil_grp.show(false)
    //df_user2_fil_part.show(false)
   val df_user2_fil_grp=df_user2_fil.groupBy("name").agg(collect_set("col")).as("languages")  
   val df_user2_fil_grp_lst=df_user2_fil.groupBy("name").agg(collect_set("col")).as("languages")
   df_user2_fil_grp.show(false)
   df_user2_fil_grp_lst.show(false)
   
  }
}