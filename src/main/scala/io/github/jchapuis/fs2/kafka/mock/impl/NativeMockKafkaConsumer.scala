package io.github.jchapuis.fs2.kafka.mock.impl

import cats.effect.IO
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.*
import io.github.jchapuis.fs2.kafka.mock.MockKafkaConsumer
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.{ Metric, MetricName, Node, PartitionInfo, TopicPartition, Uuid }

import java.time.{ Duration, Instant }
import java.util.{ Optional, OptionalLong }
import java.util.regex.Pattern
import java.{ lang, util }
import scala.annotation.nowarn
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.collection.concurrent.TrieMap
import cats.effect.unsafe.IORuntime

// the mockConsumer is not thread safe (also, all its methods are synchronized, so be careful with deadlocks),
// should only interact with it by scheduling tasks using schedulePollTask
// the poll method is where it's all happening: it will be called by the stream consuming from this mocked consumer,
// and by scheduling a task to run during `poll`, we can add records to the consumer to be returned by the poll
private[mock] class NativeMockKafkaConsumer(
  mockConsumer: MockConsumer[Array[Byte], Array[Byte]],
  topics: Seq[String]
)(implicit runtime: IORuntime)
  extends MockKafkaConsumer {
  private final val singlePartition = 0

  // configure start offsets and make sure we are assigned to the partitions
  mockConsumer.schedulePollTask { () =>
    val partitions       = topics.map(topic => new TopicPartition(topic, singlePartition))
    val beginningOffsets = partitions.map(_ -> (0L: java.lang.Long)).toMap
    mockConsumer.updateBeginningOffsets(beginningOffsets.asJava)

    mockConsumer.rebalance(topics.map(new TopicPartition(_, singlePartition)).asJava)
  }

  // create a java.util.concurrent.ConcurrentMap from topics to 0
  private val currentOffsets = TrieMap(topics.map(_ -> 0L): _*)

  def publish[K, V](topic: String, key: K, value: V, timestamp: Option[Instant])(implicit
    keySerializer: KeySerializer[IO, K],
    valueSerializer: ValueSerializer[IO, V]
  ): IO[Unit] = for {
    key   <- keySerializer.serialize(topic, Headers.empty, key)
    value <- valueSerializer.serialize(topic, Headers.empty, value)
    pf    <- IO(addRecord(topic, key, Option(value), timestamp))
  } yield ()

  private def addRecord(
    topic: String,
    key: Array[Byte],
    value: Option[Array[Byte]],
    maybeTimestamp: Option[Instant]
  ): Unit = {
    val offset = currentOffsets
      .updateWith(topic)(_.map(_ + 1))
      .getOrElse(0L)

    val timestamp = maybeTimestamp.getOrElse(Instant.now()).toEpochMilli

    val record = new org.apache.kafka.clients.consumer.ConsumerRecord[Array[Byte], Array[Byte]](
      topic,
      singlePartition,
      offset,
      timestamp,
      maybeTimestamp.map(_ => TimestampType.CREATE_TIME).getOrElse(TimestampType.LOG_APPEND_TIME),
      key.length,
      value.map(_.length).getOrElse(0),
      key,
      value.orNull,
      new RecordHeaders,
      Optional.empty[Integer]
    )

    // println(s"scheduling task for poll to return record in $topic at offset $offset")
    mockConsumer.schedulePollTask { () =>
      // println(s"adding record to topic $topic at offset $offset")
      mockConsumer.addRecord(record)
    }
  }

  def redact[K](topic: String, key: K)(implicit keySerializer: KeySerializer[IO, K]): IO[Unit] =
    for {
      key <- keySerializer.serialize(topic, Headers.empty, key)
      _   <- IO(addRecord(topic, key, None, None))
    } yield ()

  @nowarn("cat=deprecation")
  implicit lazy val mkConsumer: MkConsumer[IO] = new MkConsumer[IO] {
    private val mockFacadeWithPresetSubscriptions = new KafkaByteConsumer {
      def assignment(): util.Set[TopicPartition] = mockConsumer.assignment()

      def subscription(): util.Set[String] = mockConsumer.subscription()

      def subscribe(topics: util.Collection[String]): Unit = mockConsumer.subscribe(topics)

      def subscribe(topics: util.Collection[String], callback: ConsumerRebalanceListener): Unit =
        mockConsumer.subscribe(topics, callback)

      def assign(partitions: util.Collection[TopicPartition]): Unit = mockConsumer.assign(partitions)

      def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): Unit =
        mockConsumer.subscribe(pattern, callback)

      def subscribe(pattern: Pattern): Unit = mockConsumer.subscribe(pattern)

      def unsubscribe(): Unit = mockConsumer.unsubscribe()

      def poll(timeout: Long): ConsumerRecords[Array[Byte], Array[Byte]] = mockConsumer.poll(timeout)

      def poll(timeout: Duration): ConsumerRecords[Array[Byte], Array[Byte]] = mockConsumer.poll(timeout)

      def commitSync(): Unit = mockConsumer.commitSync()

      def commitSync(timeout: Duration): Unit = mockConsumer.commitSync(timeout)

      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = mockConsumer.commitSync(offsets)

      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata], timeout: Duration): Unit =
        mockConsumer.commitSync(offsets, timeout)

      def commitAsync(): Unit = mockConsumer.commitAsync()

      def commitAsync(callback: OffsetCommitCallback): Unit = mockConsumer.commitAsync(callback)

      def commitAsync(offsets: util.Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit =
        mockConsumer.commitAsync(offsets, callback)

      def seek(partition: TopicPartition, offset: Long): Unit = mockConsumer.seek(partition, offset)

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Unit =
        mockConsumer.seek(partition, offsetAndMetadata)

      def seekToBeginning(partitions: util.Collection[TopicPartition]): Unit = mockConsumer.seekToBeginning(partitions)

      def seekToEnd(partitions: util.Collection[TopicPartition]): Unit = mockConsumer.seekToEnd(partitions)

      def position(partition: TopicPartition): Long = mockConsumer.position(partition)

      def position(partition: TopicPartition, timeout: Duration): Long = mockConsumer.position(partition, timeout)

      def committed(partition: TopicPartition): OffsetAndMetadata = mockConsumer.committed(partition)

      def committed(partition: TopicPartition, timeout: Duration): OffsetAndMetadata =
        mockConsumer.committed(partition, timeout)

      def committed(partitions: util.Set[TopicPartition]): util.Map[TopicPartition, OffsetAndMetadata] =
        mockConsumer.committed(partitions)

      def committed(
        partitions: util.Set[TopicPartition],
        timeout: Duration
      ): util.Map[TopicPartition, OffsetAndMetadata] = mockConsumer.committed(partitions, timeout)

      def metrics(): util.Map[MetricName, _ <: Metric] = mockConsumer.metrics()

      // TODO: probably not needed
      def partitionsFor(topic: String): util.List[PartitionInfo] =
        if (mockConsumer.assignment().isEmpty) {
          util.List.of(new PartitionInfo(topic, singlePartition, null, null, null))
        } else {
          mockConsumer.partitionsFor(topic)
        }

      def partitionsFor(topic: String, timeout: Duration): util.List[PartitionInfo] = partitionsFor(topic)

      def listTopics(): util.Map[String, util.List[PartitionInfo]] = mockConsumer.listTopics()

      def listTopics(timeout: Duration): util.Map[String, util.List[PartitionInfo]] = mockConsumer.listTopics(timeout)

      def paused(): util.Set[TopicPartition] = mockConsumer.paused()

      def pause(partitions: util.Collection[TopicPartition]): Unit = mockConsumer.pause(partitions)

      def resume(partitions: util.Collection[TopicPartition]): Unit = mockConsumer.resume(partitions)

      def offsetsForTimes(
        timestampsToSearch: util.Map[TopicPartition, lang.Long]
      ): util.Map[TopicPartition, OffsetAndTimestamp] = {
        val partitions = timestampsToSearch.keySet().asScala.toList
        mockConsumer
          .beginningOffsets(
            partitions.asJava
          ) // dummy implementation as it's not supported, just returns beginning offsets
          .asScala
          .map { case (partition, offset) =>
            partition -> new OffsetAndTimestamp(offset, timestampsToSearch.get(partition))
          }
          .asJava
      }

      def offsetsForTimes(
        timestampsToSearch: util.Map[TopicPartition, lang.Long],
        timeout: Duration
      ): util.Map[TopicPartition, OffsetAndTimestamp] = offsetsForTimes(timestampsToSearch)

      def beginningOffsets(partitions: util.Collection[TopicPartition]): util.Map[TopicPartition, lang.Long] =
        mockConsumer.beginningOffsets(partitions)

      def beginningOffsets(
        partitions: util.Collection[TopicPartition],
        timeout: Duration
      ): util.Map[TopicPartition, lang.Long] = mockConsumer.beginningOffsets(partitions, timeout)

      def endOffsets(partitions: util.Collection[TopicPartition]): util.Map[TopicPartition, lang.Long] =
        mockConsumer.endOffsets(partitions)

      def endOffsets(
        partitions: util.Collection[TopicPartition],
        timeout: Duration
      ): util.Map[TopicPartition, lang.Long] = mockConsumer.endOffsets(partitions, timeout)

      def currentLag(topicPartition: TopicPartition): OptionalLong = mockConsumer.currentLag(topicPartition)

      def groupMetadata(): ConsumerGroupMetadata = mockConsumer.groupMetadata()

      def enforceRebalance(): Unit = mockConsumer.enforceRebalance()

      def enforceRebalance(reason: String): Unit = mockConsumer.enforceRebalance(reason)

      def close(): Unit = mockConsumer.close()

      def close(timeout: Duration): Unit = mockConsumer.close(timeout)

      def wakeup(): Unit = mockConsumer.wakeup()

      def clientInstanceId(timeout: Duration): Uuid = Uuid.randomUuid()
    }

    def apply[G[_]](settings: ConsumerSettings[G, ?, ?]): IO[KafkaByteConsumer] =
      IO.pure(mockFacadeWithPresetSubscriptions)
  }
}
