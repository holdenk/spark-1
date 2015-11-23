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
  val numFeatures = 5

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
    run(sc, args(0).toInt, args(1).toInt)
  }

  def run(sc: SparkContext, depth: Int, inputSize: Int): Unit = {
    val testData = makeRandomData(sc, inputSize * 5)
    val ctd = testData.map(_.features).collect()
    val pw1 = new PrintWriter(new File("warmup.csv"))
    // JVM warmup
    1.to(depth).foreach{depth => 1.to(600).foreach{trees =>
      val info = runForTrees(sc, depth, trees, ctd)
      println(s"warmuppanda,${info}")
      pw1.write(info)
    }}
    pw1.close()
    val pw2 = new PrintWriter(new File("warmup.csv"))
    // for real
    1.to(depth).foreach{depth => 1.to(600).foreach{trees =>
      val info = runForTrees(sc, depth, trees, ctd)
      println(s"livepanda,${info}")
      pw2.write(info)
    }}
    pw2.close()
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

  def runForTrees(sc: SparkContext, depth: Int, numTrees: Int,
    testData: Array[Vector]): String = {
    println(s"Generating ${numTrees} of depth ${depth}")
    val trees = 1.to(numTrees).map(x => generateTree(depth)).toArray
    val weights = 1.to(numTrees).map(x => x.toDouble / (2 * numTrees.toDouble)).toArray
    val model = new GBTClassificationModel("1", trees, weights, numFeatures)
    val broadcastModel = sc.broadcast(model)
    val codeGenModel = model.toCodeGen()
    val broadcastCodeGenModel = sc.broadcast(codeGenModel)
    val nonCodeGenTime = time(broadcastModel, testData)
    val codeGenTime = time(broadcastCodeGenModel, testData)
    s"${depth},${numTrees},${nonCodeGenTime},${codeGenTime}"
  }

  def time(model: Broadcast[GBTClassificationModel], test: Array[Vector]) = {
    val start = System.currentTimeMillis()
    1.to(20).foreach(idx => test.foreach(elem =>
      model.value.miniPredict(elem)))
    val stop = System.currentTimeMillis()
    (stop-start)
  }
}
