/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, execution, sources}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.{RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}
import org.apache.spark.sql.sources._

import scala.collection.mutable.ArrayBuffer

object AggPushDownUtils extends Logging {

  def plan(groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: LogicalPlan): Option[Seq[SparkPlan]] = {
    val (functionsWithDistinct, _) = aggregateExpressions.partition(_.isDistinct)
    if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
      // This is a sanity check. We should not reach here when we have multiple distinct
      // column sets. Our MultipleDistinctRewriter should take care this case.
      sys.error("You hit a query analyzer bug. Please report your query to " +
        "Spark user mailing list.")
    }

    if (aggregateExpressions.map(_.aggregateFunction).exists(!_.supportsPartial)
        || aggregateExpressions.map(_.aggregateFunction).exists(!_.supportsPushDown)) {
      return None
    }

    // for now, we dont support pushing down of distinct aggregation
    if (functionsWithDistinct.isEmpty) {
      val sparkPlans = planAggregateWithoutDistinct(
        groupingExpressions,
        aggregateExpressions,
        resultExpressions,
        child)
      if(sparkPlans.nonEmpty) return Some(sparkPlans)
    }

    None
  }

  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: LogicalPlan): Seq[SparkPlan] = child match {

    case PhysicalOperation(
        projects, filters, relation @ LogicalRelation(t: AggregatedFilteredScan, _, _)) =>

      val candidatePredicates = filters.map { _ transform {
        case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
      }}
      val (_, pushedFilters, handledFilters) =
        DataSourceStrategy.selectFilters(relation.relation, candidatePredicates)
      // If there are some unhandled filters, we cant perform pushed-down aggregation
      if(pushedFilters.length != handledFilters.size) return Nil

      // 1. Create an Aggregate Operator for partial aggregations.
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial))

      val output = groupingAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

      val groupingColumns = groupingAttributes.map(_.name).toArray

      val aggregateFunctions = partialAggregateExpressions
        .map(_.aggregateFunction)
        .flatMap(translateAggregateFunc)
      if(aggregateFunctions.isEmpty) return Nil

      assert(output.length == groupingColumns.length+aggregateFunctions.length)

      val metadata: Map[String, String] = buildMetadata(
        groupingColumns, aggregateFunctions, pushedFilters)

      val scan = RowDataSourceScanExec(output,
        toCatalystRDD(relation, output,
          t.buildScan(groupingColumns, aggregateFunctions.toArray, pushedFilters.toArray)),
        relation.relation,
        UnknownPartitioning(0),
        metadata,
        relation.catalogTable.map(_.identifier))

      // 2. Create an Aggregate Operator for final aggregations.
      val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      val finalAggregate = AggUtils.createAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes,
        initialInputBufferOffset = groupingExpressions.length,
        resultExpressions = resultExpressions,
        child = scan)

      finalAggregate :: Nil

    case _ => Nil

  }

  def translateAggregateFunc(func: AggregateFunction): Array[AggregateFunc] = {
    func match {
      case avg: aggregate.Average =>
        Array(sources.Sum(avg.child.asInstanceOf[NamedExpression].name, avg.sumDataType),
          sources.Count(avg.child.asInstanceOf[NamedExpression].name))
      case aggregate.Sum(child) =>
        Array(sources.Sum(child.asInstanceOf[NamedExpression].name, func.dataType))
      case aggregate.Count(children) => children.head match {
        case l: Literal => Array(sources.CountStar())
        case ne: NamedExpression => Array(sources.Count(ne.name))
        case _ =>
          logWarning(s"Unexpected children type of aggregate.Count: ${children.mkString("; ")}")
          Array.empty
      }
      case aggregate.Max(child) => Array(sources.Max(child.asInstanceOf[NamedExpression].name))
      case aggregate.Min(child) => Array(sources.Min(child.asInstanceOf[NamedExpression].name))

      case _ => Array.empty
    }
  }

  private def buildMetadata(groupingColumns: Array[String],
      aggregateFunctions: Seq[AggregateFunc],
      pushedFilters: Seq[Filter]): Map[String, String] = {
    val pairs = ArrayBuffer.empty[(String, String)]
    // Mark filters which are handled by the underlying DataSource with an Astrisk
    if (pushedFilters.nonEmpty) {
      val markedFilters = for (filter <- pushedFilters) yield {
        s"*$filter"
      }
      pairs += ("PushedFilters" -> markedFilters.mkString("[", ", ", "]"))
    }
    if (groupingColumns.nonEmpty) {
      pairs += ("GroupingColumns" -> groupingColumns.mkString(", "))
    }
    pairs += ("AggregateFunctions" -> aggregateFunctions.mkString("[", ", ", "]"))
    pairs.toMap
  }

  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   */
  private def toCatalystRDD(relation: LogicalRelation,
      output: Seq[Attribute],
      rdd: RDD[Row]): RDD[InternalRow] = {
    if (relation.relation.needConversion) {
      execution.RDDConversions.rowToRowRdd(rdd, output.map(_.dataType))
    } else {
      rdd.asInstanceOf[RDD[InternalRow]]
    }
  }

}
