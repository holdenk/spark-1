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

package org.apache.spark

import java.{lang => jl}
import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.util.Utils

/**
 * Metadata for the Accumulator
 *
 *
 * @param countFailedValues whether to accumulate values from failed tasks. This is set to true
 *                          for system and time metrics like serialization time or bytes spilled,
 *                          and false for things with absolute values like number of input rows.
 *                          This should be used for internal metrics only.

 * @param dataProperty Data property accumulators will only have values added once for each
 *                     RDD/Partition/Shuffle combination. This prevents double counting on
 *                     reevaluation. Partial evaluation of a partition will not increment a data
 *                     property accumulator. Data property accumulators are currently experimental
 *                     and the behaviour may change in future versions.
 *
 */
private[spark] case class AccumulatorMetadata(
    id: Long,
    name: Option[String],
    countFailedValues: Boolean,
    dataProperty: Boolean) extends Serializable


/**
 * The base class for accumulators, that can accumulate inputs of type `IN`, and produce output of
 * type `OUT`.
 */
abstract class NewAccumulator[IN, OUT] extends Serializable {
  private[spark] var metadata: AccumulatorMetadata = _
  private[this] var atDriverSide = true

  /**
   * The following values are used for data property [[NewAccumulator]]s.
   * Data property [[NewAccumulator]]s have only-once semantics. These semantics are implemented
   * by keeping track of which RDD id, shuffle id, and partition id the current function is
   * processing in. If a partition is fully processed the results for that partition/shuffle/rdd
   * combination are sent back to the driver. The driver keeps track of which rdd/shuffle/partitions
   * already have been applied, and only combines values into value_ if the rdd/shuffle/partition
   * has not already been aggregated on the driver program
   */
  // For data property accumulators pending and processed updates.
  // Pending and processed are keyed by (rdd id, shuffle id, partition id)
  @transient private[spark] lazy val pending =
    new mutable.HashMap[(Int, Int, Int), NewAccumulator[IN, OUT]]()
  // Completed contains the set of (rdd id, shuffle id, partition id) that have been
  // fully processed on the worker side. This is used to determine if the updates should
  // be sent back to the driver for a particular rdd/shuffle/partition combination.
  @transient private[spark] lazy val completed = new mutable.HashSet[(Int, Int, Int)]()
  // Processed is keyed by (rdd id, shuffle id) and the value is a bitset containing all partitions
  // for the given key which have been merged into the value. This is used on the driver.
  @transient private[spark] lazy val processed = new mutable.HashMap[(Int, Int), mutable.BitSet]()

  private[spark] def dataProperty: Boolean = metadata.dataProperty

  private[spark] def register(
      sc: SparkContext,
      name: Option[String] = None,
      countFailedValues: Boolean = false,
      dataProperty: Boolean = false): Unit = {
    if (this.metadata != null) {
      throw new IllegalStateException("Cannot register an Accumulator twice.")
    }
    this.metadata = AccumulatorMetadata(AccumulatorContext.newId(), name, countFailedValues,
      dataProperty)
    AccumulatorContext.register(this)
    sc.cleaner.foreach(_.registerAccumulatorForCleanup(this))
  }

  /**
   * Returns true if this accumulator has been registered.  Note that all accumulators must be
   * registered before ues, or it will throw exception.
   */
  final def isRegistered: Boolean =
    metadata != null && AccumulatorContext.get(metadata.id).isDefined

  private def assertMetadataNotNull(): Unit = {
    if (metadata == null) {
      throw new IllegalAccessError("The metadata of this accumulator has not been assigned yet.")
    }
  }

  /**
   * Returns the id of this accumulator, can only be called after registration.
   */
  final def id: Long = {
    assertMetadataNotNull()
    metadata.id
  }

  /**
   * Returns the name of this accumulator, can only be called after registration.
   */
  final def name: Option[String] = {
    assertMetadataNotNull()
    metadata.name
  }

  /**
   * Whether to accumulate values from failed tasks. This is set to true for system and time
   * metrics like serialization time or bytes spilled, and false for things with absolute values
   * like number of input rows.  This should be used for internal metrics only.
   */
  private[spark] final def countFailedValues: Boolean = {
    assertMetadataNotNull()
    metadata.countFailedValues
  }

  /**
   * Creates an [[AccumulableInfo]] representation of this [[NewAccumulator]] with the provided
   * values.
   */
  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    val isInternal = name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))
    new AccumulableInfo(id, name, update, value, isInternal, countFailedValues)
  }

  final private[spark] def isAtDriverSide: Boolean = atDriverSide

  /**
   * Tells if this accumulator is zero value or not. e.g. for a counter accumulator, 0 is zero
   * value; for a list accumulator, Nil is zero value.
   */
  def isZero(): Boolean

  /**
   * Creates a new copy of this accumulator, which is zero value. i.e. call `isZero` on the copy
   * must return true.
   */
  def copyAndReset(): NewAccumulator[IN, OUT]

  /**
   * Takes the inputs and accumulates. e.g. it can be a simple `+=` for counter accumulator.
   * Developers should extend addImpl to customize the adding functionality.
   */
  final def add(v: IN): Unit = {
    assertMetadataNotNull()
    addImpl(v)
    if (metadata.dataProperty) {
      val updateInfo = TaskContext.get().getRDDPartitionInfo()
      val base = pending.getOrElse(updateInfo, copyAndReset())
      base.addImpl(v)
    }
  }

  /**
   * Mark a specific rdd/shuffle/partition as completely processed. This is a noop for
   * non-data property accumuables.
   */
  private[spark] def markFullyProcessed(rddId: Int, shuffleWriteId: Int, partitionId: Int): Unit = {
    if (metadata.dataProperty) {
      completed += ((rddId, shuffleWriteId, partitionId))
    }
  }

  /**
   * Takes the inputs and accumulates. e.g. it can be a simple `+=` for counter accumulator.
   * Developers should extend addImpl to customize the adding functionality.
   */
  protected def addImpl(v: IN)

  /**
   * Merges another same-type accumulator into this one and update its state, i.e. this should be
   * merge-in-place. Developers should extend mergeImpl to customize the adding functionality.
   */
  final private[spark] def merge(other: NewAccumulator[IN, OUT]): Unit = {
    assertMetadataNotNull()
    // Handle data property accumulators
    if (metadata.dataProperty) {
      val term = other.pending
      term.foreach { case ((rddId, shuffleWriteId, splitId), v) =>
        val splits = processed.getOrElseUpdate((rddId, shuffleWriteId), new mutable.BitSet())
        if (!splits.contains(splitId)) {
          splits += splitId
          mergeImpl(v)
        }
      }
    } else {
      mergeImpl(other)
    }
  }

  /**
   * Merges another same-type accumulator into this one and update its state, i.e. this should be
   * merge-in-place. Developers should extend mergeImpl to customize the adding functionality.
   */
  protected def mergeImpl(other: NewAccumulator[IN, OUT]): Unit

  /**
   * Access this accumulator's current value; only allowed on driver.
   */
  final def value: OUT = {
    if (atDriverSide) {
      localValue
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }

  /**
   * Defines the current value of this accumulator.
   *
   * This is NOT the global value of the accumulator.  To get the global value after a
   * completed operation on the dataset, call `value`.
   */
  def localValue: OUT

  // Called by Java when serializing an object
  final protected def writeReplace(): Any = {
    if (atDriverSide) {
      if (!isRegistered) {
        throw new UnsupportedOperationException(
          "Accumulator must be registered before send to executor")
      }
      val copy = copyAndReset()
      assert(copy.isZero(), "copyAndReset must return a zero value copy")
      copy.metadata = metadata
      copy
    } else {
      this
    }
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    if (atDriverSide) {
      atDriverSide = false

      // Automatically register the accumulator when it is deserialized with the task closure.
      // This is for external accumulators and internal ones that do not represent task level
      // metrics, e.g. internal SQL metrics, which are per-operator.
      val taskContext = TaskContext.get()
      if (taskContext != null) {
        taskContext.registerAccumulator(this)
      }
    } else {
      atDriverSide = true
    }
  }

  override def toString: String = {
    if (metadata == null) {
      "Un-registered Accumulator: " + getClass.getSimpleName
    } else {
      getClass.getSimpleName + s"(id: $id, name: $name, value: $localValue)"
    }
  }
}


private[spark] object AccumulatorContext {

  /**
   * This global map holds the original accumulator objects that are created on the driver.
   * It keeps weak references to these objects so that accumulators can be garbage-collected
   * once the RDDs and user-code that reference them are cleaned up.
   * TODO: Don't use a global map; these should be tied to a SparkContext (SPARK-13051).
   */
  @GuardedBy("AccumulatorContext")
  private val originals = new java.util.HashMap[Long, jl.ref.WeakReference[NewAccumulator[_, _]]]

  private[this] val nextId = new AtomicLong(0L)

  /**
   * Return a globally unique ID for a new [[Accumulator]].
   * Note: Once you copy the [[Accumulator]] the ID is no longer unique.
   */
  def newId(): Long = nextId.getAndIncrement

  def numAccums: Int = synchronized(originals.size)

  def accumIds: Set[Long] = synchronized(originals.keySet().asScala.toSet)

  /**
   * Register an [[Accumulator]] created on the driver such that it can be used on the executors.
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what [[org.apache.spark.scheduler.DAGScheduler]] does.
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
   *
   * If an [[Accumulator]] with the same ID was already registered, this does nothing instead
   * of overwriting it. We will never register same accumulator twice, this is just a sanity check.
   */
  def register(a: NewAccumulator[_, _]): Unit = synchronized {
    if (!originals.containsKey(a.id)) {
      originals.put(a.id, new jl.ref.WeakReference[NewAccumulator[_, _]](a))
    }
  }

  /**
   * Unregister the [[Accumulator]] with the given ID, if any.
   */
  def remove(id: Long): Unit = synchronized {
    originals.remove(id)
  }

  /**
   * Return the [[Accumulator]] registered with the given ID, if any.
   */
  def get(id: Long): Option[NewAccumulator[_, _]] = synchronized {
    Option(originals.get(id)).map { ref =>
      // Since we are storing weak references, we must check whether the underlying data is valid.
      val acc = ref.get
      if (acc eq null) {
        throw new IllegalAccessError(s"Attempted to access garbage collected accumulator $id")
      }
      acc
    }
  }

  /**
   * Clear all registered [[Accumulator]]s. For testing only.
   */
  def clear(): Unit = synchronized {
    originals.clear()
  }
}

class LongAccumulator extends NewAccumulator[jl.Long, jl.Long] {
  private[this] var _sum = 0L

  override def isZero(): Boolean = _sum == 0

  override def copyAndReset(): LongAccumulator = new LongAccumulator

  override def addImpl(v: jl.Long): Unit = _sum += v

  def add(v: Long): Unit = _sum += v

  def sum: Long = _sum

  override def mergeImpl(other: NewAccumulator[jl.Long, jl.Long]): Unit = other match {
    case o: LongAccumulator => _sum += o.sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Long): Unit = _sum = newValue

  override def localValue: jl.Long = _sum
}


class DoubleAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  private[this] var _sum = 0.0

  override def isZero(): Boolean = _sum == 0.0

  override def copyAndReset(): DoubleAccumulator = new DoubleAccumulator

  override def addImpl(v: jl.Double): Unit = _sum += v

  def add(v: Double): Unit = _sum += v

  def sum: Double = _sum

  override def mergeImpl(other: NewAccumulator[jl.Double, jl.Double]): Unit = other match {
    case o: DoubleAccumulator => _sum += o.sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Double): Unit = _sum = newValue

  override def localValue: jl.Double = _sum
}


class AverageAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  private[this] var _sum = 0.0
  private[this] var _count = 0L

  override def isZero(): Boolean = _sum == 0.0 && _count == 0

  override def copyAndReset(): AverageAccumulator = new AverageAccumulator

  override def addImpl(v: jl.Double): Unit = {
    _sum += v
    _count += 1
  }

  def add(d: Double): Unit = {
    _sum += d
    _count += 1
  }

  override def mergeImpl(other: NewAccumulator[jl.Double, jl.Double]): Unit = other match {
    case o: AverageAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def localValue: jl.Double = if (_count == 0) {
    Double.NaN
  } else {
    _sum / _count
  }

  def sum: Double = _sum

  def count: Long = _count
}


class ListAccumulator[T] extends NewAccumulator[T, java.util.List[T]] {
  private[this] val _list: java.util.List[T] = new java.util.ArrayList[T]

  override def isZero(): Boolean = _list.isEmpty

  override def copyAndReset(): ListAccumulator[T] = new ListAccumulator

  override def addImpl(v: T): Unit = _list.add(v)

  override def mergeImpl(other: NewAccumulator[T, java.util.List[T]]): Unit = other match {
    case o: ListAccumulator[T] => _list.addAll(o.localValue)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def localValue: java.util.List[T] = java.util.Collections.unmodifiableList(_list)

  private[spark] def setValue(newValue: java.util.List[T]): Unit = {
    _list.clear()
    _list.addAll(newValue)
  }
}


class LegacyAccumulatorWrapper[R, T](
    initialValue: R,
    param: org.apache.spark.AccumulableParam[R, T]) extends NewAccumulator[T, R] {
  private[spark] var _value = initialValue  // Current value on driver

  override def isZero(): Boolean = _value == param.zero(initialValue)

  override def copyAndReset(): LegacyAccumulatorWrapper[R, T] = {
    val acc = new LegacyAccumulatorWrapper(initialValue, param)
    acc._value = param.zero(initialValue)
    acc
  }

  override def addImpl(v: T): Unit = _value = param.addAccumulator(_value, v)

  override def mergeImpl(other: NewAccumulator[T, R]): Unit = other match {
    case o: LegacyAccumulatorWrapper[R, T] => _value = param.addInPlace(_value, o.localValue)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def localValue: R = _value
}
