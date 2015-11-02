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

package org.apache.spark.ml.feature

import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{IntParam, _}
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.random.XORShiftRandom

/**
 * Params for [[QuantileDiscretizer]].
 */
private[feature] trait QuantileDiscretizerBase extends Params with HasInputCol with HasOutputCol {

  /**
   * Maximum number of buckets (quantiles, or categories) into which data points are grouped. Must
   * be >= 2.
   * default: 2
   * @group param
   */
  val numBuckets = new IntParam(this, "numBuckets", "Maximum number of buckets (quantiles, or " +
    "categories) into which data points are grouped. Must be >= 2.",
    ParamValidators.gtEq(2))
  setDefault(numBuckets -> 2)

  /** @group getParam */
  def getNumBuckets: Int = getOrDefault(numBuckets)
}

/**
 * :: Experimental ::
 * `QuantileDiscretizer` takes a column with continuous features and outputs a column with binned
 * categorical features. The bin ranges are chosen by taking a sample of the data and dividing it
 * into roughly equal parts. The lower and upper bin bounds will be -Infinity and +Infinity,
 * covering all real values. This attempts to find numBuckets partitions based on a sample of data,
 * but it may find fewer depending on the data sample values.
 */
@Experimental
final class QuantileDiscretizer(override val uid: String)
  extends Estimator[Bucketizer] with QuantileDiscretizerBase {

  def this() = this(Identifiable.randomUID("quantileDiscretizer"))

  /** @group setParam */
  def setNumBuckets(value: Int): this.type = set(numBuckets, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType)
    val inputFields = schema.fields
    require(inputFields.forall(_.name != $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  override def fit(dataset: DataFrame): Bucketizer = {
    val samples = QuantileDiscretizer.getSampledInput(dataset.select($(inputCol)), $(numBuckets))
      .map { case Row(feature: Double) => feature }
    val candidates = QuantileDiscretizer.findSplitCandidates(samples, $(numBuckets) - 1)
    val splits = QuantileDiscretizer.getSplits(candidates)
    val bucketizer = new Bucketizer(uid).setSplits(splits)
    copyValues(bucketizer)
  }

  override def copy(extra: ParamMap): QuantileDiscretizer = defaultCopy(extra)
}

private[feature] object QuantileDiscretizer extends Logging {
  /**
   * Sampling from the given dataset to collect quantile statistics.
   */
  def getSampledInput(dataset: DataFrame, numBins: Int): DataFrame = {
    val totalSamples = dataset.count()
    require(totalSamples > 0,
      "QuantileDiscretizer requires non-empty input dataset but was given an empty input.")
    val requiredSamples = math.max(numBins * numBins, 10000)
    val fraction = math.min(requiredSamples / dataset.count(), 1.0)
    dataset.sample(withReplacement = false, fraction, new XORShiftRandom().nextInt())
  }

  /**
   * Compute split points with respect to the sample distribution.
   */
  def findSplitCandidates(samples: RDD[Double], numSplits: Int): Array[Double] = {
    val valueCountRDD = samples.map((_, 1)).reduceByKey(_ + _)
    val sortedValueCounts = valueCountRDD.sortByKey()
    sortedValueCounts.cache()
    val sampleCountPerPartition = sortedvalues.mapPartitions(itr =>
      itr.map(_._2).reduce(_ + _)).collect()
    val possibleSplits = sortedValueCounts.count()
    // If the number of unique values is less than requested splits just return the values in order
    if (possibleSplits <= numSplits) {
      valueCounts.collect().map(_._1)
    } else {
      val indexes = 1.to(numSplits).map((_ * possibleSplits / numSplits).toInt)
      val previousPartitionsCount = sampleCountPerPartition.scanLeft(0)(_ + _)
      val partition
    }
  }

  /**
   * Adjust split candidates to proper splits by: adding positive/negative infinity to both sides as
   * needed, and adding a default split value of 0 if no good candidates are found.
   */
  def getSplits(candidates: Array[Double]): Array[Double] = {
    val effectiveValues = if (candidates.size != 0) {
      if (candidates.head == Double.NegativeInfinity
        && candidates.last == Double.PositiveInfinity) {
        candidates.drop(1).dropRight(1)
      } else if (candidates.head == Double.NegativeInfinity) {
        candidates.drop(1)
      } else if (candidates.last == Double.PositiveInfinity) {
        candidates.dropRight(1)
      } else {
        candidates
      }
    } else {
      candidates
    }

    if (effectiveValues.size == 0) {
      Array(Double.NegativeInfinity, 0, Double.PositiveInfinity)
    } else {
      Array(Double.NegativeInfinity) ++ effectiveValues ++ Array(Double.PositiveInfinity)
    }
  }
}
