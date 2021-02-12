package ca.cn.otds.spark.common

import org.apache.spark.sql._

object DataFrameUtils {
  implicit class DataFrameImprovements(df: org.apache.spark.sql.DataFrame) {
    def profile : DataFrame = {

      df.cache
      lazy val spark = SparkSession.builder().getOrCreate()

      import spark.implicits._

      val profile : Seq[ColumnProfile] = Seq(
        df.columns.toSeq.map(col => ColumnProfile.ColumnFuncs(col,df))
      ).flatten

      profile.toDF()
    }
  }
}
