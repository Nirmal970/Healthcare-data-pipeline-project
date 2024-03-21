import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object task_t2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("task_t2")
      .getOrCreate()
      
    val datasets = List(
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

    datasets.foreach(dataset => {
      val inputPath = s"/home/hadoop/health_datasets/$dataset"

      val data = spark.read.option("header", "true").csv(inputPath)
      val entitiesToRemove = List(
        "High income",
        "Low and middle income",
        "Low income",
        "Lower middle income",
        "Middle income",
        "Upper middle income"
      )

      val filteredData = data.filter(col("Entity").isin(entitiesToRemove: _*))
      val outputPath = s"/home/hadoop/health_datasets_updated/$dataset"
      filteredData.write.option("header", "true").csv(outputPath)
    })

    spark.stop()
  }
}