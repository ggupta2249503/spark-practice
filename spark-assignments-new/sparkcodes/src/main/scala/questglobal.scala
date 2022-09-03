import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object questglobal {
  def questFunc(spark:SparkSession){
   
    val schema=new StructType().add("id",IntegerType).add("priority",StringType).add("status",StringType).add("datetime",TimestampType).
    add("data_as_of_Date",TimestampType).add("Amount",IntegerType)
    val df=spark.read.schema(schema).csv("src/main/resources/quest.txt")toDF("id","priority","status","datetime","data_as_of_Date","Amount")
    df.show(5,false)
    
    /*1. Create a column name open_close
	If value of status column is in (new , investigating , fix in progress) then open_close should have value as ‘Open’
	If value of status column is in (Fixed) then open_close column should have value as ‘Closed'*/
    val lst_status=Array("New" , "Investigating" , "Fix in progress")
    val df1=df.withColumn("open_close",when(trim(col("status")).isInCollection(lst_status),"Open").when(trim(col("status"))==="Fixed","Closed"))
    df1.show(false)
    
    //2. If value of status column is in (Removed) then all rows having same id will be removed from dataframe. 
    //val df2=df.select(col("status")==="Removed")
    val df2_lst_row=df.filter(trim(col("status"))==="Removed")
    df2_lst_row.show(false)
    import spark.implicits._
    val df2_lst_ids=df2_lst_row.select("id").map(x=>x.getInt(0)).collect()
    df2_lst_ids.foreach(x=>println(x))
    val df_drop=df.filter(col("id").isInCollection(df2_lst_ids))
    df_drop.show(false)
    val df_min=df.except(df_drop)
    df_min.show(false)
    
    
//3. Calculate the difference between datetime column and data_as_of_date column In days and store it in date_diff
    val df_3=df.withColumn("date_diff",to_date(col("datetime"))-to_date(col("data_as_of_Date")))
    val df_3_1=df.withColumn("date_diff1",datediff(to_date(col("datetime")),to_date(col("data_as_of_Date"))))
    df_3_1.show(false)
    
//4. create a auto increment column which can act as primary key in db

val window=Window.orderBy(col("id"))
val df_row=df.withColumn("row_no",row_number.over(window))
df_row.show(false)
//https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6

val rdd=df.rdd.zipWithIndex.map(x=>(x._1.getInt(0),x._1.getString(1),x._1.getString(2),x._1.getTimestamp(3),x._1.getTimestamp(4),x._1.getInt(5),x._2)).toDF
//rdd.collect.foreach(println)
rdd.show()


    
  
  
   
  
    
  }
}