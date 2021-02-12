# df_profiler
spark dataframe profiler

Have created a wrapper on top of the dataframe profile script provided by Vikram.
eg usage :
import ca.cn.otds.spark.common.DataFrameUtils._
dataframe.sample(0.1).profile.show()
output :
+--------------------+--------+---------+-------------+----------+----------+--------------------+--------------------+
| colName|colCount|nullCount|distinctCount|minColSize|maxColSize| minValue| maxValue|
+--------------------+--------+---------+-------------+----------+----------+--------------------+--------------------+
| CRCHeader| 911| 911| 1| 0| 0| [null]| [null]|
| CarID| 911| 0| 2| 10| 10| [null]| [null]|
| DataCategory| 911| 0| 3| 12| 18| [null]| [null]|
| DecimalMilepost| 911| 0| 37| 4| 9| [-1.0]| [175.99873]|
| Direction| 911| 0| 1| 3| 3| [null]| [null]|
| GISPackageVersion| 911| 0| 1| 14| 14| [20200130144645]| [20200130144645]|
