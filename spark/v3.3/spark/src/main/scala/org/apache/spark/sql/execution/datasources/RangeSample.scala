/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources

import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.random.SamplingUtils
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32


class RangeSample[K: ClassTag](
                                zorderPartitions: Int,
                                rdd: RDD[Seq[K]],
                                val samplePointsPerPartitionHint: Int
                              ) extends Serializable {


  import scala.collection.mutable.ArrayBuffer
  // TODO 改一改
  // We allow zEncodeNum = 0, which happens when sorting an empty RDD under the default settings.
  require(zorderPartitions >= 0, s"Number of partitions cannot be negative but found $zorderPartitions.")

  def getRangeBounds: ArrayBuffer[(Seq[K], Float)] = {
    if (zorderPartitions <= 1) {
      import scala.collection.mutable.ArrayBuffer
      ArrayBuffer.empty[(Seq[K], Float)]
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // Cast to double to avoid overflowing ints or longs
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * zorderPartitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = sketch(rdd, sampleSizePerPartition)
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
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        candidates
      }
    }
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
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
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
}

object RangeSampleSort {

  import org.apache.spark.sql.DataFrame

  def getRangeBound(df: DataFrame,
                    zOrderField: Seq[StructField],
                    zOrderBounds: Int,
                    samplePointsPerPartitionHint: Int): Array[Array[_ >: String with Long]] = {
    import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
    import scala.jdk.CollectionConverters.asJavaIterableConverter

    val zOrderIndexField = zOrderField.map { field =>
      field.dataType match {
        case LongType | BooleanType |
             DoubleType | FloatType | StringType | IntegerType | DateType | TimestampType | ShortType | ByteType =>
          (df.schema.fields.indexOf(field), field)
        case _: DecimalType =>
          (df.schema.fields.indexOf(field), field)
        case _ =>
          throw new IllegalArgumentException(String.format("Unsupported type for " + field.dataType))
      }
    }

    val sampleRdd = df.rdd.map { row: Row =>
      val values = zOrderIndexField.map { case (index, field) =>
        field.dataType match {
          case LongType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getLong(index)
          case DoubleType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getDouble(index).toLong
          case IntegerType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getInt(index).toLong
          case FloatType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getFloat(index).toLong
          case StringType =>
            if (row.isNullAt(index)) "" else row.getString(index)
          case DateType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getDate(index).getTime
          case TimestampType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getTimestamp(index).getTime
          case ByteType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getByte(index).toLong
          case ShortType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getShort(index).toLong
          case decimalType: DecimalType =>
            if (row.isNullAt(index)) Long.MaxValue else row.getDecimal(index).longValue()
          case _ =>
            null
        }
      }
      values
    }

    val sample = new RangeSample(zOrderBounds, sampleRdd, samplePointsPerPartitionHint)
    // get all samples
    val candidates: mutable.Seq[(Seq[Any], Float)] = sample.getRangeBounds
    // calculates boundZ
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
      }
    }
    sampleBounds.asJava.toArray
  }
}

