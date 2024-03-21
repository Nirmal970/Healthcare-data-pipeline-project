import org.apache.spark.sql.{SparkSession, DataFrame}

object task_t3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("task_t3")
      .getOrCreate()


    val transformedDatasets = List(
      "annual-healthcare-expenditure-per-capita.csv",
      "expenditure-of-the-national-health-service-nhs-in-the-uk.csv",
      "health-insurance-coverage-in-the-us.csv",
      "health-protection-coverage.csv",
      "percentage-of-persons-without-health-insurance-coverage-us.csv",
      "public-health-expenditure-share-GDP-OWID.csv",
      "public-health-insurance-coverage.csv",
      "public-healthcare-spending-share-gdp.csv",
      "share-of-out-of-pocket-expenditure-on-healthcare.csv",
      "share-of-public-expenditure-on-healthcare-by-country.csv",
      "total-healthcare-expenditure-gdp.csv"
    )


    transformedDatasets.foreach(transformedDataset => {

      val inputPath = s"/home/hadoop/health_datasets_updated/$transformedDatasets"

      val transformedData = spark.read.csv(inputPath)

      val s3OutputPath = s"s3n://bunny970/Datasets_updated//$transformedDataset"

      transformedData.write.csv(s3OutputPath)
    })

    spark.stop()
  }
}