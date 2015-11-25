package org.apache.spark.ml.examples

import scala.collection.mutable
import scala.language.reflectiveCalls
import org.apache.spark.ml.attribute._
import org.apache.spark.sql.types._

import org.apache.spark.ml.regression._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.tree.configuration._
import scala.collection.JavaConverters._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.tree._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.mllib.linalg.Vectors

import java.io._

object GBT {

  val numFeatures = 5000

  def makeRandomData(sc: SparkContext, size: Int): RDD[LabeledPoint] = {
    val vectors = RandomRDDs.normalVectorRDD(sc, size, numFeatures)
    val labels = RandomRDDs.normalVectorRDD(sc, size, 1)
    val labelVecs = labels.zip(vectors)
    labelVecs.map{case(a, b) =>
      val label = if(a(0) > 0.5) 0.0 else 1.0
      new LabeledPoint(label, b)}
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GBT")
    val sc = new SparkContext(conf)
    run(sc, args(0).toInt, args(1).toInt, args(2).toInt)
  }

  def run(sc: SparkContext, depth: Int, numTrees: Int, inputSize: Int): Unit = {
    val testData = makeRandomData(sc, inputSize).map(_.features)
    val ctd = sc.broadcast(testData.collect())
    2.to(depth).foreach{depth =>
      val pw1 = new PrintWriter(new File(s"warmup_${depth}.csv"))
      pw1.write("depth,numTrees,localNonCodeGenTime,localCodeGenTime\n")
      val resultStrs = sc.parallelize(1.to(numTrees, 5)).repartition(16).map{trees =>
        runForTrees(depth, trees, ctd)
      }.collect()
      pw1.write(resultStrs.mkString("\n"))
      pw1.close()
    }
  }

  val rand = new scala.util.Random()

  def generateNode(depth: Int): Node = {
    if (depth == 0) {
      new LeafNode(rand.nextDouble(), rand.nextDouble(), null)
    } else {
      new InternalNode(rand.nextDouble(), rand.nextDouble(),
        rand.nextDouble(), generateNode(depth - 1),
        generateNode(depth - 1), new ContinuousSplit(
          (rand.nextDouble() * numFeatures).toInt,
          rand.nextDouble()),
        null)
    }
  }

  def generateTree(depth: Int): DecisionTreeRegressionModel = {
    new DecisionTreeRegressionModel("murh", generateNode(depth), numFeatures)
  }

  def runForTrees(depth: Int, numTrees: Int, testData: Broadcast[Array[Vector]]): String = {
    println(s"Generating ${numTrees} of depth ${depth}")
    val trees = 1.to(numTrees).map(x => generateTree(depth)).toArray
    val weights = 1.to(numTrees).map(x => x.toDouble / (2 * numTrees.toDouble)).toArray
    val model = new GBTClassificationModel("1", trees, weights, numFeatures)
    val codeGenModel = model.toCodeGen()
    val tdv = testData.value
    val codeGenTime = time(codeGenModel, tdv)
    val nonCodeGenTime = time(model, tdv)
    s"${depth},${numTrees},${nonCodeGenTime},${codeGenTime}"
  }

  def time(model: GBTClassificationModel,
    test: Array[Vector]) = {
    // JVM warmup
    1.to(400).foreach(idx =>
      test.foreach(elem =>
        model.miniPredict(elem)))
    // RL
    val localStart = System.currentTimeMillis()
    1.to(10).foreach(idx =>
      test.foreach(elem =>
        model.miniPredict(elem)))
    val localStop = System.currentTimeMillis()
    (localStop-localStart)
  }
}
