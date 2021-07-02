/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.kylin.engine.spark.job

import java.util
import java.util.Locale

import com.google.common.base.Joiner
import com.google.common.collect.{Lists, Maps}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.builder.CreateFlatTable
import org.apache.kylin.engine.spark.utils.{SchemaProcessor, SparkConfHelper}
import org.apache.kylin.metadata.model.{ColumnDesc, TableDesc}
import org.apache.kylin.metadata.project.ProjectManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkTypeUtil
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

class TableAnalysisJob(tableDesc: TableDesc,
                       project: String,
                       rowCount: Long,
                       ss: SparkSession,
                       jobId: String) extends Serializable with Logging {

  // it's a experimental value recommended by Spark,
  // which used for controlling the TableSampling tasks' count.
  val taskFactor = 4

  def getSampleTableDataSet(): Dataset[Row] = {
    val columnDescs: Array[ColumnDesc] = tableDesc.getColumns
    val tblColNames: util.List[String] = Lists.newArrayListWithCapacity(columnDescs.length)
    var kylinSchema: StructType = new StructType
    for (columnDesc <- columnDescs) {
      if (!columnDesc.isComputedColumn) {
        kylinSchema = kylinSchema.add(columnDesc.getName, SchemaProcessor.toSparkType(columnDesc.getType), nullable = true)
        tblColNames.add("`" + columnDesc.getName + "`")
      }
    }
    val colString: String = Joiner.on(",").join(tblColNames)
    val sql: String = String.format(Locale.ROOT, "select %s from %s", colString, tableDesc.getIdentity)
    val df: Dataset[Row] = ss.sql(sql)
    val sparkSchema: StructType = df.schema
    log.debug("Source data sql is: {}", sql)
    log.debug("Kylin schema: {}", kylinSchema.treeString)
    val columns: Array[Column] = SparkTypeUtil.alignDataType(sparkSchema, kylinSchema)
    val sparkConf = ss.sparkContext.getConf

    val instances = sparkConf.get(SparkConfHelper.EXECUTOR_INSTANCES, "1").toInt
    val cores = sparkConf.get(SparkConfHelper.EXECUTOR_CORES, "1").toInt
    val numPartitions = instances * cores
    val params = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv)
      .getProject(tableDesc.getProject).getOverrideKylinProps
    params.put("sampleRowCount", String.valueOf(rowCount))
    val dataFrame = df.select(columns: _*).coalesce(numPartitions)
    CreateFlatTable.changeSchemaToAliasDotName(dataFrame, tableDesc.getIdentity)
  }

  def analyzeTable(sampledDataset: Dataset[Row]): Array[Row] = {
    // todo: use sample data to estimate total info
    // calculate the stats info
    val statsMetrics = buildStatsMetric(sampledDataset)
    val aggData: Array[Row] = sampledDataset.agg(count(lit(1)), statsMetrics: _*).collect()
    val sampleData: Array[Row] = sampledDataset.limit(10).collect()
    aggData ++ sampleData
  }

  def buildStatsMetric(sourceTable: Dataset[Row]): scala.List[Column] = {
    sourceTable.schema.fieldNames.flatMap(
      name =>
        Seq(TableAnalyzerJob.TABLE_STATS_METRICS.toArray(): _*).map {
          case "COUNT" =>
            count(col(name))
          case "COUNT_DISTINCT" =>
            approx_count_distinct(col(name))
          case "MAX" =>
            max(col(name))
          case "MIN" =>
            min(col(name))
          case _ =>
            throw new IllegalArgumentException(
              s"""Unsupported metric in TableSampling """)
        }
    ).toList
  }

  //Gets the value that appears most frequently in a particular column in the table
  def buildHighFrequency(sourceTable: Dataset[Row], column: String, allCount: Long, frequency: Double): util.Map[java.lang.String, java.lang.Long] = {
    val map: util.Map[java.lang.String, java.lang.Long] = Maps.newHashMap()
    val row: Array[Row] = sourceTable.groupBy(column)
      .agg(count(column))
      .filter(count(column)/allCount > frequency).collect()
    row.foreach(x => map.put(x.getString(0), x.getLong(1)))
    map
  }

}
