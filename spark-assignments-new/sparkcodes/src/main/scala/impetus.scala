import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object impetus {
  def impetus_function(spark:SparkSession){
    // read 2 files and count the no. of  unique words
     //val df_sample1=spark.read.option("delimiter", ",").text("src/main/resources/sample1.txt")
    val df_sample1_csv=spark.read.csv("src/main/resources/sample1.txt").toDF("id","word")
    //df_sample1.show()
    df_sample1_csv.show()
   
    val df_sample2_csv=spark.read.csv("src/main/resources/sample2.txt").toDF("id","word")
    
    df_sample2_csv.show()
    val df_union=df_sample1_csv.union(df_sample2_csv)
    df_union.show()
    val df_union_sel=df_union.select(col("word"))
    val  df_union_sel_grp=df_union_sel.groupBy(col("word")).count()
    df_union_sel_grp.show(false)
    val lst1=List(1,2,3)
    import spark.implicits._
    val lst2=List(2,3,4)
    val union_df=lst1.toDF("id").union(lst2.toDF("id"))
    union_df.show()
  }
}