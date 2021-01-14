package SparkScala
import schema.CustomerSchema
import utilities.Functions
object CustomerCsvToSolution {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Users\\i.sbrizzi\\hadoop")

    val funObj = new Functions()
    val schema=new CustomerSchema().schema
    val spark= funObj.setSparkSession()
    //val df=funObj.read(spark,"C:\\Users\\i.sbrizzi\\Desktop\\Dati ENEL\\customer.csv", "csv", ";", schema)
    //val dfLowerCase=funObj.computeLowerCase(df)
    //val dfWithoutEmpty=funObj.blankAsNull(dfLowerCase)
    //funObj.writeToPostgres(dfWithoutEmpty,"customer")


    //funObj.writeToS3(dfWithoutEmpty,"csv","s3a://rio-data-complete/rio-data/prova.parquet")

    funObj.readFromS3(spark,"parquet","s3a://rio-data-complete/rio-data/prova.parquet")

  }
}
