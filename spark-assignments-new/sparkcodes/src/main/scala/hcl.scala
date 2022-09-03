
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.spark_partition_id

object hcl {
  def hclFunc(spark:SparkSession){
    
    /*Order table:
order    date        status
ts1        21-01-2021    open
ts2        13-02-2021    inprogress
ts3        16-02-2021    completed
ts4        21-03-2021    cancelled
ts5        29-04-2021    open
ts6        30-05-2021    inprogress

 

delta records:
order    date        status
ts1        22-01-2021    inprogress
ts2        16-02-2021    completed
ts2        17-02-2021    cancelled
ts6        15-06-2021    completed*/
    
val list1=List(("ts1","21-01-2021","open"),("ts2","13-02-2021","inprogress"),("ts3","16-02-2021","completed"),("ts4","21-03-2021","cancelled"),("ts5","29-04-2021","open"),("ts6","30-05-2021","inprogress"))

val list2=List(("ts1","22-01-2021","inprogress"),("ts2","16-02-2021","completed"),("ts2","17-02-2021","cancelled"),("ts6","15-06-2021","completed"))


import spark.implicits._
val df1=list1.toDF("order_name","order_date","status")
val df2=list2.toDF("order_name","order_date","status")
val cols=df1.columns
val cols_dtypes=df1.dtypes
cols.foreach(x=>println(x))
cols_dtypes.foreach(y=>println(y))

val df_union=df1.union(df2)
df_union.createOrReplaceTempView("table")
val df_dense_rank=spark.sqlContext.sql("select order_name,order_date,status,dense_rank() over (partition by order_name order by order_date desc) as dense_rank from table")
df_dense_rank.show()
val df_dense_rank_fil=df_dense_rank.filter(col("dense_rank")===1)
df_dense_rank_fil.show()

// to display order only upto 31st march 2022
val list3= List(Row("ts1","21-01-2022","open"),Row("ts2","13-02-2022","inprogress"),Row("ts3","16-02-2022","completed"),Row("ts4","21-03-2022","cancelled"),Row("ts5","29-04-2022","open"),Row("ts6","30-05-2022","inprogress"),Row("ts6","31-03-2022","inprogress"))

val schema=StructType(Array(StructField("order_name",StringType),StructField("order_date",StringType),StructField("status",StringType)))
val rdd3=spark.sparkContext.parallelize(list3)
val df3=spark.createDataFrame(rdd3, schema)
df3.show()

val mon=3
val year=2022
val dat=31
val df3_fil=df3.filter{x=>
  var lst=x.getString(1).split("-")
  lst(0).toInt<=dat && lst(1).toInt<=mon && lst(2).toInt<=year}
df3_fil.show()

val df4=df3.repartition(3)
// to display no. of patitions and count of records in each partition
println(df4.rdd.getNumPartitions)
val df5=df4.toDF().withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count()
df5.show()

// add column a and b if not null otherwise display as 0
val list_ele=List(Row(1,2),Row(3,4),Row(null,1))
val rdd_ele=spark.sparkContext.parallelize(list_ele)
val schema1=StructType(Array(StructField("a",IntegerType,true),StructField("b",IntegerType,true)))
val df_ele=spark.createDataFrame(rdd_ele,schema1)
df_ele.show()
val df_ele_new=df_ele.withColumn("c",when(col("a").isNotNull && col("b").isNotNull,col("a")+col("b")).otherwise(0) )
df_ele_new.show()

//read the file and get the highest and lowest value of a and b

val df_file=spark.read.textFile("src/main/resources/sample5.txt")

df_file.show(false)
val df_file_split=df_file.toDF("lines").map{x=>
  var lst=x.getString(0).split(",")
  (lst(0),lst.slice(1,lst.length))
}.toDF("id","values")
df_file_split.show(false)
val df_file_split_exp=df_file_split.select(col("id"),explode(col("values")))
df_file_split_exp.show(false)
val df_file_split_exp_grp=df_file_split_exp.groupBy(col("id")).agg(min(col("col")), max(col("col")))
df_file_split_exp_grp.show(false)

  }
}