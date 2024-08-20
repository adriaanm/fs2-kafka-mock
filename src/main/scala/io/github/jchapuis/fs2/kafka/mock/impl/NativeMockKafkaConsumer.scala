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

private[mock] class NativeMockKafkaConsumer(
  val mockConsumer: MockConsumer[Array[Byte], Array[Byte]],
  topics: Seq[String]
)(implicit runtime: IORuntime)
  extends MockKafkaConsumer {

  {
    val partitions       = topics.map(topic => new TopicPartition(topic, 0))
    val beginningOffsets = partitions.map(_ -> (0L: java.lang.Long)).toMap
    mockConsumer.updateBeginningOffsets(beginningOffsets.asJava)
  }

  private val singlePartition = 0

  // create a java.util.concurrent.ConcurrentMap from topics to 0
  private val currentOffsets = TrieMap(topics.map(_ -> 0L): _*)

  def publish[K, V](topic: String, key: K, value: V, timestamp: Option[Instant])(implicit
    keySerializer: KeySerializer[IO, K],
    valueSerializer: ValueSerializer[IO, V]
  ): IO[Unit] = for {
    _     <- IO.println(s"publishing to topic $topic under key $key with value $value")
    key   <- keySerializer.serialize(topic, Headers.empty, key)
    value <- valueSerializer.serialize(topic, Headers.empty, value)
    _     <- IO(addRecord(topic, key, Option(value), timestamp))
    _     <- IO.sleep(500.milli) // TODO: why is this needed?? cede before and after doesn't do anything
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

    println(s"scheduling record to topic $topic at offset $offset")

    mockConsumer.schedulePollTask { () =>
      println(s"adding record to topic $topic at offset $offset")
      mockConsumer.addRecord(record)
    }
  }

  def redact[K](topic: String, key: K)(implicit keySerializer: KeySerializer[IO, K]): IO[Unit] =
    for {
      key <- keySerializer.serialize(topic, Headers.empty, key)
      _   <- IO(addRecord(topic, key, None, None))
    } yield ()

  private def withMutex[T](f: => T): T = f // synchronized(f)

  @nowarn("cat=deprecation")
  implicit lazy val mkConsumer: MkConsumer[IO] = new MkConsumer[IO] {
    private val mockFacadeWithPresetSubscriptions = new KafkaByteConsumer {
      def assignment(): util.Set[TopicPartition] = withMutex(mockConsumer.assignment())

      def subscription(): util.Set[String] = withMutex(mockConsumer.subscription())

      def subscribe(topics: util.Collection[String]): Unit = withMutex {
        println(s"subscribing to $topics")
        mockConsumer.subscribe(topics)
        ensureConsumerAssignedTo(topics.asScala.toList)
      }

      def subscribe(topics: util.Collection[String], callback: ConsumerRebalanceListener): Unit = withMutex {
        mockConsumer.subscribe(topics, callback)
        ensureConsumerAssignedTo(topics.asScala.toList)
      }

      def assign(partitions: util.Collection[TopicPartition]): Unit = withMutex(mockConsumer.assign(partitions))

      def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): Unit =
        withMutex(mockConsumer.subscribe(pattern, callback))

      def subscribe(pattern: Pattern): Unit = withMutex(mockConsumer.subscribe(pattern))

      def unsubscribe(): Unit = withMutex(mockConsumer.unsubscribe())

      def poll(timeout: Long): ConsumerRecords[Array[Byte], Array[Byte]] = withMutex(mockConsumer.poll(timeout))

      def poll(timeout: Duration): ConsumerRecords[Array[Byte], Array[Byte]] = withMutex {
        val res = mockConsumer.poll(timeout)
        println(s"polling result: ${res.iterator().asScala.toList}")
        res
      }

      def commitSync(): Unit = withMutex(mockConsumer.commitSync())

      def commitSync(timeout: Duration): Unit = withMutex(mockConsumer.commitSync(timeout))

      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = withMutex(
        mockConsumer.commitSync(offsets)
      )

      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata], timeout: Duration): Unit =
        withMutex(mockConsumer.commitSync(offsets, timeout))

      def commitAsync(): Unit = withMutex(mockConsumer.commitAsync())

      def commitAsync(callback: OffsetCommitCallback): Unit = withMutex(mockConsumer.commitAsync(callback))

      def commitAsync(offsets: util.Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit =
        withMutex(mockConsumer.commitAsync(offsets, callback))

      def seek(partition: TopicPartition, offset: Long): Unit = withMutex(mockConsumer.seek(partition, offset))

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Unit =
        withMutex(mockConsumer.seek(partition, offsetAndMetadata))

      def seekToBeginning(partitions: util.Collection[TopicPartition]): Unit =
        withMutex(mockConsumer.seekToBeginning(partitions))

      def seekToEnd(partitions: util.Collection[TopicPartition]): Unit = withMutex(
        mockConsumer.seekToEnd(partitions)
      )

      def position(partition: TopicPartition): Long = withMutex(mockConsumer.position(partition))

      def position(partition: TopicPartition, timeout: Duration): Long = withMutex(
        mockConsumer.position(partition, timeout)
      )

      def committed(partition: TopicPartition): OffsetAndMetadata = withMutex(mockConsumer.committed(partition))

      def committed(partition: TopicPartition, timeout: Duration): OffsetAndMetadata =
        withMutex(mockConsumer.committed(partition, timeout))

      def committed(partitions: util.Set[TopicPartition]): util.Map[TopicPartition, OffsetAndMetadata] =
        withMutex(mockConsumer.committed(partitions))

      def committed(
        partitions: util.Set[TopicPartition],
        timeout: Duration
      ): util.Map[TopicPartition, OffsetAndMetadata] = withMutex(mockConsumer.committed(partitions, timeout))

      def metrics(): util.Map[MetricName, _ <: Metric] = withMutex(mockConsumer.metrics())

      def partitionsFor(topic: String): util.List[PartitionInfo] =
        withMutex {
          if (mockConsumer.assignment().isEmpty) {
            util.List.of(new PartitionInfo(topic, singlePartition, null, null, null))
          } else {
            mockConsumer.partitionsFor(topic)
          }
        }

      def partitionsFor(topic: String, timeout: Duration): util.List[PartitionInfo] = partitionsFor(topic)

      def listTopics(): util.Map[String, util.List[PartitionInfo]] = withMutex(mockConsumer.listTopics())

      def listTopics(timeout: Duration): util.Map[String, util.List[PartitionInfo]] = withMutex(
        mockConsumer.listTopics(timeout)
      )

      def paused(): util.Set[TopicPartition] = withMutex(mockConsumer.paused())

      def pause(partitions: util.Collection[TopicPartition]): Unit = withMutex(mockConsumer.pause(partitions))

      def resume(partitions: util.Collection[TopicPartition]): Unit = withMutex(mockConsumer.resume(partitions))

      def offsetsForTimes(
        timestampsToSearch: util.Map[TopicPartition, lang.Long]
      ): util.Map[TopicPartition, OffsetAndTimestamp] = withMutex {
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
        withMutex(mockConsumer.beginningOffsets(partitions))

      def beginningOffsets(
        partitions: util.Collection[TopicPartition],
        timeout: Duration
      ): util.Map[TopicPartition, lang.Long] = withMutex(mockConsumer.beginningOffsets(partitions, timeout))

      def endOffsets(partitions: util.Collection[TopicPartition]): util.Map[TopicPartition, lang.Long] =
        withMutex(mockConsumer.endOffsets(partitions))

      def endOffsets(
        partitions: util.Collection[TopicPartition],
        timeout: Duration
      ): util.Map[TopicPartition, lang.Long] = withMutex(mockConsumer.endOffsets(partitions, timeout))

      def currentLag(topicPartition: TopicPartition): OptionalLong = withMutex(
        mockConsumer.currentLag(topicPartition)
      )

      def groupMetadata(): ConsumerGroupMetadata = withMutex(mockConsumer.groupMetadata())

      def enforceRebalance(): Unit = withMutex(mockConsumer.enforceRebalance())

      def enforceRebalance(reason: String): Unit = withMutex(mockConsumer.enforceRebalance(reason))

      def close(): Unit = withMutex(mockConsumer.close())

      def close(timeout: Duration): Unit = withMutex(mockConsumer.close(timeout))

      def wakeup(): Unit = withMutex(mockConsumer.wakeup())

      def clientInstanceId(timeout: Duration): Uuid = Uuid.randomUuid()
    }

    private def ensureConsumerAssignedTo(topics: List[String]): Unit =
      mockConsumer.rebalance(topics.map(new TopicPartition(_, singlePartition)).asJava)

    def apply[G[_]](settings: ConsumerSettings[G, ?, ?]): IO[KafkaByteConsumer] =
      IO.pure(mockFacadeWithPresetSubscriptions)
  }
}
