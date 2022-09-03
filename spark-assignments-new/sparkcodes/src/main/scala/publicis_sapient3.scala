import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object publicis_sapient3 {
  
  def windowFunctions(spark:SparkSession){
    
    val lst_fruits=List(("banana",2,"fruit"),("apple",8,"fruit"),("leek",2,"vegetable"),("cabbage",9,"vegetable"),("lettuce",10,"vegetable"),("kale",23,"vegetable"))
    val cols=List("item","purchases","category")
    import spark.implicits._
    val fruits_df=lst_fruits.toDF("item","purchases","category")
    fruits_df.show(false)
    fruits_df.createOrReplaceTempView("fruits")
/* expected output 1st part   +-------------------------------------------------------+
| item | purchases | category | total_purchases |
+-------------------------------------------------------+
| banana | 2 | fruit | 10 |
| apple | 8 | fruit | 10 |
| leek | 2 | vegetable | 44 |
| cabbage | 9 | vegetable | 44 |
| lettuce | 10 | vegetable | 44 |
| kale | 23 | vegetable | 44 |
+-------------------------------------------------------+ */
    
    val fruits_df_mod=spark.sqlContext.sql("select item,purchases,category,sum(purchases) over(partition by category) as total_purchases from fruits order by total_purchases")
    fruits_df_mod.show(false)
    
    
    

/*expected output 2nd part| item | purchases | category | total_purchases |
+-------------------------------------------------------+
| banana | 2 | fruit | 2 |
| apple | 8 | fruit | 10 |
| leek | 2 | vegetable | 2 |
| cabbage | 9 | vegetable | 11 |
| lettuce | 10 | vegetable | 21 |
| kale | 23 | vegetable | 44 |
+-------------------------------------------------------+ */

    val fruits_df_mod2=spark.sqlContext.sql("select item,purchases,category,sum(purchases) over(partition by category order by purchases rows between unbounded preceding and current row) as total_purchases from fruits order by category")
    fruits_df_mod2.show(false)
    val fruits_df_mod3=spark.sqlContext.sql("select item,purchases,category,sum(purchases) over(partition by category  rows between unbounded preceding and current row) as total_purchases from fruits ")
    fruits_df_mod3.show(false)
    val fruits_df_mod4=spark.sqlContext.sql("select item,purchases,category,sum(purchases) over(partition by category  rows between unbounded preceding and current row) as total_purchases from fruits order by category")
    fruits_df_mod4.show(false)
    val fruits_df_mod5=spark.sqlContext.sql("select item,purchases,category,sum(purchases) over(partition by category  rows between unbounded preceding and 0 following) as total_purchases from fruits order by category")
    fruits_df_mod5.show(false)
    
    //expected output -top 4 populous states
    val list_states=List((1,"A"),(2,"A"),(3,"A"),(4,"A"),(5,"A"),(65,"A"),(67,"A"),(6,"B"),(7,"B"),(8,"B"),(9,"B"),(0,"B"),(56,"B"),(10,"C"),(11,"C"),(12,"C"),(92,"C"),(87,"C"),(13,"D"),(14,"D"),(17,"F"))
    val df_states=list_states.toDF("aadharid","statecode")
    df_states.createOrReplaceTempView("states")
    val df_states_pop=spark.sqlContext.sql("select statecode,count(aadharid) over(partition by statecode) as total_pop from states")
    df_states_pop.show()
    df_states_pop.createOrReplaceTempView("states_pop")
    val df_states_pop_rank=spark.sqlContext.sql("select statecode,total_pop,dense_rank() over( order by total_pop desc) as dense_rank from states_pop")
    df_states_pop_rank.show()
    val df_states_pop_rank_dis=df_states_pop_rank.filter(col("dense_rank")<=4).select(col("statecode")).distinct()
    df_states_pop_rank_dis.show()
    val df_states_pop1=spark.sqlContext.sql("select statecode,count(aadharid) as count from states group by statecode   order by count desc")
    df_states_pop1.show()
    
  }
}