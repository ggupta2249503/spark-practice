import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object cgi {
  def cgiFunctions(spark:SparkSession){
    
    val lst=List(1,2,3,-9,-7,-6)
    //Find sum of positive & negative values in a column separately in scala spark?
    import spark.implicits._
    val df_pos=lst.toDF("id").filter(col("id")>0)
    val df_pos_sum=df_pos.agg(sum(col("id")))
    df_pos_sum.show()
    val df_neg=lst.toDF("id").filter(col("id")<0)
    val df_neg_sum=df_neg.agg(sum(col("id")))
    df_neg_sum.show()
    //val df1=lst.toDF("id").withColumn("
    
   /* Need to add new column to this dataframe avoiding '-' in column "ac"
input:
+------------+--------+-------------+
| ac| drv|systemaccount|
+------------+--------+-------------+
|0031-662-050|31662050| BBK0092486|
|0031-437-001|31437001| TTP0092486|
+------------+--------+-------------+*/
    
    val list_accnts=List(("0031-662-050","31662050","BBK0092486"),("0031-437-001","31437001","TTP0092486"))
    val df_accnts=list_accnts.toDF("ac","drv","systemaccount")
    val df_accnts_mod=df_accnts.map{x=>
      var lst=x.getString(0).split("-")
      var str=(lst(0).concat(lst(1)).concat(lst(2)))
      (x.getString(0),x.getString(1),x.getString(2),str.slice(2,str.length ))
    }
    df_accnts_mod.show()
    
    
    //replace all names like lnt,lti,ltts with LNT
    
    val list_comp=List(("gaurav","lti"),("gupta","lnt"),("kumar","ltts"),("aayush","lndt"),("guddu","tata"),("psycho","genpact"))
    var lst_lt=List("lti","lnt","ltts","lndt")
    val df_comp=list_comp.toDF("name","comp")
    
    val df_comp_mod=df_comp.withColumn("newcomp",when(col("comp").isInCollection(lst_lt),"LNT").otherwise(col("comp")))
    df_comp_mod.show()
    val df_comp_mod1=df_comp.withColumn("comp",when(col("comp").isInCollection(lst_lt),"LNT").otherwise(col("comp")))
    df_comp_mod1.show()
    
    
    
  }
}