package ca.cn.otds.spark.common

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

case class ColumnProfile(colName: String, colCount: Long, nullCount: Long,
                         distinctCount: Long, minColSize: String, maxColSize: String,
                         minValue: String, maxValue: String) {
  override def toString: String = List(
    colName
    , colCount
    , nullCount
    , distinctCount
    , minColSize
    , maxColSize
    , minValue
    , maxValue
  ).mkString(";")
}

object ColumnProfile {

  def isNumeric(col: String): Boolean = {
    val isNum: Option[Float] = Try(col.toFloat).toOption
    isNum.isDefined
  }
  val udfIsNumeric: UserDefinedFunction = udf[Boolean, String](isNumeric)

  def ColumnFuncs(inCol:String,df:DataFrame): ColumnProfile = {
    val colName = df.select(col(inCol))
    colName.cache()
    val colCount = colName.count()
    val nullCounts = colName.filter(col(inCol).isNull or col(inCol) === "").count()
    val distinctCount = colName.distinct().count()
    val colSize = colName.withColumn("colLen",length(col(inCol))).cache()
    val minSize = colSize.agg(min(col("colLen"))).drop(col(inCol)).collect()(0).getInt(0)
    val maxSize = colSize.agg(max(col("colLen"))).drop(col(inCol)).collect()(0).getInt(0)
    val colVals = colName.withColumn("colValues",when(udfIsNumeric(col(inCol)),true)
      .otherwise(false)).filter(col("colValues")===true).drop("colValues").cache()
    val minValue = colVals.agg(min(col(inCol))).drop(col(inCol)).collect().headOption.getOrElse(0)
    val maxValue = colVals.agg(max(col(inCol))).drop(col(inCol)).collect().headOption.getOrElse(0)

    ColumnProfile(inCol,colCount,nullCounts,distinctCount,minSize.toString,maxSize.toString,minValue.toString,maxValue.toString)
  }
}