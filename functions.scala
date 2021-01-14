package utilities
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
class Functions {

  def setSparkSession() :SparkSession={
      val spark=SparkSession
        .builder
        .appName("App: Loading Customer CSV")
        .master("local[*]")
        .getOrCreate
      return spark
  }


  def read(spark: SparkSession, pathIn: String, format:String, delimiter:String, schema: StructType): DataFrame={
      val df=spark
        .read
        .format(format)
        .option("header", "true")
        .option("sep", delimiter)
        .option("treatEmptyValuesAsNulls","true")
        .option("nullValue", null)
        .option("inferSchema", "true")
        .schema(schema)
        .option("dateFormat", "MM/dd/yyyy")
        .load(pathIn)

      df.printSchema()
      df.show(false)
      return df

  }

  def readFromS3(spark: SparkSession, format:String, pathIn: String): DataFrame ={
    val df=spark
      .read
      .format(format)
      .option("header", "true")
      .load(pathIn)

    df.printSchema()
    df.show(false)
    return df

  }

  def parsingSchema(df: DataFrame): Array[String] ={
    val findIDs =
      for (c <- df.columns
           if c.contains("ID") || c.contains("id"))
        yield c

    findIDs.foreach(println)
    return findIDs

  }

  def checkKeySubsetUniqueness(df:DataFrame): Array[String] ={
    val findIDs=parsingSchema(df)
    for(i<-findIDs){
      val dfWithCountDup=df.groupBy(i).count().sort("count")
      dfWithCountDup.show(false)

      df.groupBy(i).count().where(col("count")>1)
        .sort(col("count").desc)
        .show(false)
    }

    /*for (i<- findIDs){
      val windowSpec  = Window.partitionBy(i).orderBy("Insertion_Date")
      val dfWithRowNumber=df.withColumn("row_number",row_number.over(windowSpec)).show()
      //dfWithRowNumber.filter(dfWithRowNumber("row_number")===1)
    }*/
    return findIDs
  }



  def searchingDuplicates(df:DataFrame): Array[String] ={
    val findIDs=parsingSchema(df)
    for(i<-findIDs){
      val dfDistCount=df.dropDuplicates(i).count()
      val dfCount=df.select(i).count()
      println("-- Distinct Count for the Primary Key: "+i+" : "+
        dfDistCount +" on Total Dataset : " +dfCount)
      if(dfDistCount==dfCount){
        println("No Duplicates Found!")
      }
    }
    return findIDs
  }




  def computeSubsetUniqueness(df:DataFrame): Array[String] ={
    val findIDs=searchingDuplicates(df)
    for (i<- findIDs; j<-findIDs){
      if(i!=j){
        val dfDistCount=df.dropDuplicates(i,j).count()
        val dfCount=df.select(i,j).count()

        println("-- Distinct Count for the Primary Keys: "+i,j+" : "+
          dfDistCount +" on Total Dataset : " +dfCount)
        if(dfDistCount==dfCount){
          println("No Duplicates Found!")
        }
      }
    }
    return findIDs
  }

  def searchingNullValues(df:DataFrame): Unit ={
    val findIDs=parsingSchema(df)
    for (i<- findIDs) {
      df.filter(col(i).isNull).show()

    }
  }

  def searchingSpecialChar(df:DataFrame): Unit ={
    df.filter(col("Result_detailed").like("%�%"))

  }

  def treatingSpecialChar(df: DataFrame): Unit ={
    df.withColumn("SPIDNew", regexp_replace(df("SPID"), "\\Õ", "O"))
  }


  def computeLowerCase(df: DataFrame): DataFrame={
    val df1=df.select(df.columns.map(x => col(x).as(x.toLowerCase)): _*)
    df1.show(false)

  return df1
  }
  def blankAsNull(df: DataFrame):DataFrame={
    val df1 = df.na.replace(df.columns,Map(" " -> null))
    df1.show()
    return df1

    /*val df1=df.withColumn("business_unofficial_name",
      when(col("business_unofficial_name").equalTo(" "), null))*/
  }

  def write(df: DataFrame,pathOut: String){
    df.write
      .format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue", null)
      .option("dateFormat", "MM/dd/yyyy")
      .mode("overwrite")
      .save(pathOut)
  }

  def writeToPostgres(df:DataFrame, tableName:String): Unit ={
    df.write
      .mode("overwrite")
      .option("dbtable",tableName)
      .option("url", "jdbc:postgresql://localhost/postgres")
      .option("driver", "org.postgresql.Driver")
      .option("user", "postgres")
      .option("password", "admin")
      .format("jdbc")
      .save()
  }

  def writeToS3(df:DataFrame,format:String, pathOut:String): Unit ={
    df.repartition(10)
      .write
      .format(format)
      .option("header", "true")
      .option("sep", ";")
      .mode("overwrite")
      .save(pathOut)
  }

  def closeSparkSession(spark: SparkSession): Unit ={
    spark.stop
  }


  def generateEventsFile(df:DataFrame): DataFrame ={
    val df1=df
      //.withColumn("New1", regexp_replace(df("EventDate"), " ", "T"))
      //.withColumn("New2", functions.concat(col("New1"), lit(".000Z")))
      //.drop("New1","EventDate")
      //.withColumnRenamed("New2","EventDate")
      .withColumn("List",functions.concat(lit('{'),col("EventDate"),lit(','),
        col("Var"),lit(','),col("Value"),lit('}')))
      .drop("EventDate","Var","Value")

    //df1.show(false)

    val df2=df1.groupBy("SPID")
      .agg(concat_ws(",", collect_set("List")).as("event_history"))

    //df2.show(false)

    val df3=df2
      .withColumn("event_hist",functions.concat(lit('['),col("event_history"),lit(']')))
      .drop("event_history")

    df3.show(false)

    return df3
  }



}
