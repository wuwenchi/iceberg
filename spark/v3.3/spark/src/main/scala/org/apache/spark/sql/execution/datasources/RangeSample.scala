/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources

import org.apache.iceberg.spark.actions.SparkZOrderUDF
import org.apache.iceberg.util.ZOrderByteUtils
import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types._
import org.apache.spark.util.MutablePair
import org.apache.spark.util.random.SamplingUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

class RangeSample[K: ClassTag, V](
                                   zEncodeNum: Int,
                                   rdd: RDD[Seq[K]],
                                   val samplePointsPerPartitionHint: Int
                                 ) extends Serializable {

  import scala.collection.mutable.ArrayBuffer

  // We allow zEncodeNum = 0, which happens when sorting an empty RDD under the default settings.
  require(zEncodeNum >= 0, s"Number of zEncodeNum cannot be negative but found $zEncodeNum.")
  require(samplePointsPerPartitionHint > 0,
    s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

  def getRangeBounds(): ArrayBuffer[(Seq[K], Float)] = {
    if (zEncodeNum <= 1) {
      import scala.collection.mutable.ArrayBuffer
      ArrayBuffer.empty[(Seq[K], Float)]
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // Cast to double to avoid overflowing ints or longs
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * zEncodeNum, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      //      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = sketch(rdd, sampleSizePerPartition)
      //      val (numItems, sketched) = sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        ArrayBuffer.empty[(Seq[K], Float)]
      } else {
        import scala.collection.mutable
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(Seq[K], Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]

        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }

        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd, imbalancedPartitions.contains)
          //          val imbalanced: PartitionPruningRDD[K] =
          //              new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          //          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        candidates
      }
    }
  }

  def determineBounds[K: Ordering : ClassTag](candidates: ArrayBuffer[(K, Float)], partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   *
   * @param candidates unordered candidates with weights
   * @param partitions number of partitions
   * @return selected bounds
   */
  def determineBound[K: Ordering : ClassTag](
                                              candidates: ArrayBuffer[(K, Float)],
                                              partitions: Int, ordering: Ordering[K]): Array[K] = {
    val ordered = candidates.sortBy(_._1)(ordering)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }


  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd                    the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K: ClassTag](
                           rdd: RDD[K],
                           sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }
}

class RawDecisionBound[K: Ordering : ClassTag](ordering: Ordering[K]) extends Serializable {

  private var binarySearch: ((Array[K], K) => Int) = {
    import scala.reflect.classTag
    // For primitive keys, we can use the natural ordering. Otherwise, use the Ordering comparator.
    classTag[K] match {
      case ClassTag.Float =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Float]], x.asInstanceOf[Float])
      case ClassTag.Double =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Double]], x.asInstanceOf[Double])
      case ClassTag.Byte =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Byte]], x.asInstanceOf[Byte])
      case ClassTag.Char =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Char]], x.asInstanceOf[Char])
      case ClassTag.Short =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Short]], x.asInstanceOf[Short])
      case ClassTag.Int =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Int]], x.asInstanceOf[Int])
      case ClassTag.Long =>
        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[Long]], x.asInstanceOf[Long])
      case _ =>
        null
      //        val comparator = ordering.asInstanceOf[java.util.Comparator[Any]]
      // TODO 这个干啥的？
      //        (l, x) => java.util.Arrays.binarySearch(l.asInstanceOf[Array[AnyRef]], x, comparator)
    }
  }

  def getBound(key: Any, candidateBounds: Array[K]): Int = {
    val k = key.asInstanceOf[K]
    var bound = 0
    if (candidateBounds.length <= 128) {
      while (bound < candidateBounds.length && ordering.gt(k, candidateBounds(bound))) {
        bound += 1
      }
    } else {
      bound = binarySearch(candidateBounds, k)
      if (bound < 0) {
        bound = -bound - 1
      }
      if (bound > candidateBounds.length) {
        bound = candidateBounds.length
      }
    }
    bound
  }
}

//case class ByteArraySorting(b: Array[Byte]) extends Ordered[ByteArraySorting] with Serializable {
//  override def compare(that: ByteArraySorting): Int = {
//    val len = this.b.length
//    BinaryUtil.compareTo(this.b, 0, len, that.b, 0, len)
//  }
//}

object RangeSampleSort {

  def determineBounds[K: Ordering : ClassTag](candidates: ArrayBuffer[(K, Float)], partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }

  import org.apache.spark.sql.DataFrame

  import scala.collection.immutable

  def abcde(): Unit = {
    //    val tuples = new ArrayBuffer[(String, Float)]()
    //    tuples.append(("asdfa", 1.0f), ("e", 2.0f), ("nk", 3.0f), ("sb", 2.0f))
    //    val strings = RangeSampleSort.determineBounds(tuples, 3)
    //    print(strings.mkString("Array(", ", ", ")"))
    //    val bytes = "1".getBytes()
    //    print(bytes)
    //    val tuples = new ArrayBuffer[(Array[Byte], Float)]()
    //    RangeSampleSort.determineBounds(tuples, 3)


  }

  def getRangeBound(df: DataFrame,
                    zOrderField: Seq[StructField],
                    zOrderBounds: Int)
  : Array[Array[_ >: String with Long]] = {
    import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
    import scala.collection.immutable
    import scala.jdk.CollectionConverters.asJavaIterableConverter

    val zOrderIndexField = zOrderField.map { field =>
      field.dataType match {
        // TODO 比 hudi 多一个binary，少一个 DecimalType
        case LongType | BooleanType | BinaryType |
             DoubleType | FloatType | StringType | IntegerType | DateType | TimestampType | ShortType | ByteType => true
          (df.schema.fields.indexOf(field), field)
        case _ =>
          throw new IllegalArgumentException(String.format("sdfsdfsdf"))
      }
    }

    val sampleRdd: RDD[Seq[Any]] = df.rdd.map { row: Row =>
      val values = zOrderIndexField.map { case (index, field) =>
        field.dataType match {
          case LongType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getLong(index)
          case DoubleType =>
            if (row.isNullAt(index)) Double.MaxValue else row.getDouble(index)
          case IntegerType =>
            if (row.isNullAt(index)) Int.MaxValue else row.getInt(index)
          case FloatType =>
            if (row.isNullAt(index)) Float.MaxValue else row.getFloat(index)
          case StringType =>
            if (row.isNullAt(index)) "" else row.getString(index)
          case DateType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getDate(index).getTime
          case TimestampType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getTimestamp(index).getTime
          case ByteType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getByte(index)
          case ShortType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getShort(index)
          //          // TODO 需要支持吗？
          //          case DecimalType =>
          //            if (row.isNullAt(index)) Long.MaxValue else row.getDecimal(index)
          // TODO get BinaryType
          //          case BinaryType =>
          //            if (row.isNullAt(index)) "" else row.getAs[Array[Byte]](index)
          case _ =>
            null
        }
      }
      values
    }

    //    sampleRdd.saveAsTextFile("/Users/wuwenchi/github/iceberg/temp/RDD/sampleRdd")

    val hint = 5
    val sample = new RangeSample(zOrderBounds, sampleRdd, hint)
    // get all samples
    val candidates = sample.getRangeBounds()
    // calculates bound
    val sampleBounds = {
      val candidateColNumber = candidates.head._1.length
      (0 until candidateColNumber).map { idx =>
        val colRangeBound = candidates.map(x => (x._1(idx), x._2))

        if (colRangeBound.head._1.isInstanceOf[String]) {
          sample.determineBounds(
            colRangeBound.asInstanceOf[ArrayBuffer[(String, Float)]],
            math.min(zOrderBounds, candidates.length))
        } else {
          sample.determineBounds(
            colRangeBound.asInstanceOf[ArrayBuffer[(Long, Float)]],
            math.min(zOrderBounds, candidates.length))
        }

        //        colRangeBound.head._1 match {
        //          case String =>
        //            sample.determineBounds(
        //              colRangeBound.asInstanceOf[ArrayBuffer[(String, Float)]],
        //              math.min(zOrderBounds, candidates.length))
        //          case Long =>
        //            sample.determineBounds(colRangeBound.asInstanceOf[ArrayBuffer[(Long, Float)]], zOrderBounds)
        //          case Double =>
        //            sample.determineBounds(colRangeBound.asInstanceOf[ArrayBuffer[(Double, Float)]], zOrderBounds)
        //          case Int =>
        //            sample.determineBounds(colRangeBound.asInstanceOf[ArrayBuffer[(Int, Float)]], zOrderBounds)
        //          case Float =>
        //            sample.determineBounds(colRangeBound.asInstanceOf[ArrayBuffer[(Float, Float)]], zOrderBounds)
        //          case Byte =>
        //            sample.determineBounds(colRangeBound.asInstanceOf[ArrayBuffer[(Byte, Float)]], zOrderBounds)
        //          case Short =>
        //            sample.determineBounds(colRangeBound.asInstanceOf[ArrayBuffer[(Short, Float)]], zOrderBounds)
        ////          case DecimalType =>
        ////            sample.determineBounds(colRangeBound.asInstanceOf[ArrayBuffer[(Decimal, Float)]], zOrderBounds)
        //
        //          // TODO 需要支持吗？
        //            case BinaryType =>
        //              sample.determineBounds(colRangeBound.asInstanceOf[ArrayBuffer[(binary, Float)]],zOrderBounds)
        //        }
      }
    }
    sampleBounds.asJava.toArray
  }
}

