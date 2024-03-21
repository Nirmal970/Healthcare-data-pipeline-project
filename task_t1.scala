import org.apache.spark.sql.SparkSession

object task_t1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("task_t1")
      .getOrCreate()

    val s3BasePath = "s3n://bunny970/Datasets/"

    val datasets = List("annual-healthcare-expenditure-per-capita.csv", "expenditure-of-the-national-health-service-nhs-in-the-uk.csv", 
    "health-insurance-coverage-in-the-us.csv", "health-protection-coverage.csv", "percentage-of-persons-without-health-insurance-coverage-us.csv", 
    "public-health-expenditure-share-GDP-OWID.csv", "public-health-insurance-coverage.csv", 
    "public-healthcare-spending-share-gdp.csv", "share-of-out-of-pocket-expenditure-on-healthcare.csv", "share-of-public-expenditure-on-healthcare-by-country.csv", 
    "total-healthcare-expenditure-gdp.csv")

    datasets.foreach(dataset => {
      val s3Path = s"$s3BasePath/$dataset"
      val data = spark.read.csv(s3Path)
      val outputPath = s"/home/hadoop/health_datasets/$dataset"
    })

    spark.stop()
  }
}
