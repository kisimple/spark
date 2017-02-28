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

package org.apache.spark.sql.sources

import org.apache.spark.sql.types.DataType

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines all the aggregate functions that we can push down to the data sources.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * aggregate function for data sources.
 */
abstract class AggregateFunc {}

/**
 * Here we now have three data types: LongType, DoubleType and DecimalType. <br/>
 * And the result types should be java.lang.Long, java.lang.Double and java.math.BigDecimal
 * respectively.
 */
case class Sum(column: String, dataType: DataType) extends AggregateFunc {}

/**
 * The result type of Count MUST be java.lang.Long
 */
case class Count(column: String) extends AggregateFunc {}

case class CountStar() extends AggregateFunc {}

case class Max(column: String) extends AggregateFunc {}

case class Min(column: String) extends AggregateFunc {}
