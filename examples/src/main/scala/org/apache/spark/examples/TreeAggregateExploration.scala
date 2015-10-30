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

package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDDSuiteUtils._
import org.apache.spark.util.Utils
import org.apache.spark.mllib.random.RandomRDDs

/**
 * Explore the behaviour of TreeAggregation using a SparkListener
 */
object TreeAggregateExploration {
  def main(args: Array[String]) {
    val maxDepth = args(0).toInt
    val baseSize = args(1).toInt
    val scale = args(2).toInt
    val maxPartitions = args(3).toInt
    val runs = args(4).toInt
    val sparkConf = new SparkConf().setAppName("TreeAggregateExploration")
    val data = 1.to(maxDepth) flatMap {depth =>
      1.to(maxPartitions) map {partitions =>
        val sc = new SparkContext(sparkConf)
        // Do some warmup
        val junk = sc.parallelize(1.to(1000))
        junk.treeReduce(_ + _)
        junk.reduce(_ + _)
        val key = (depth, partitions)
        (key,
          explore(sc, depth, size * (1+scale * partitions),
            partitions))
        sc.stop()
      }
    }
    println(s"scale ${scale}")
    println("depth, partitions, shuffle read, shuffle write, execution time")
    data.map(case (k, v) => k.toList.mkString(",") + "," +
      v.toList.mkStirng(","))
    println(data)
  }

  def explore(sc, depth: Int, size: int, partitions: Int, runs: Int) = {
    // Create a new listener for each exploration
    val listener = new MiniListener()
    sc.addSparkListener(listener)
    // Run the same job multiple times just to validate
    val startTime = System.nanoTime()
    1.to(runs).foreach{ r =>
      val data = RanomdRDDs.normalRDD(sc, size, partitions)
      data.treeReduce(_ + _)
    }
    val endTime = System.nanoTime()
    val wallTime = endTime - startTime
    List(listener.totalExecutorRunTime, listener.shuffledWrite, wallTime)
  }
}

class MiniListener extends SparkListener {
  var totalExecutorRunTime = 0L
  var shuffledWrite = 0L
}
