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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.io.kafka8;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.kafka8.KafkaCheckpointMark.PartitionMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.ExposedByteArrayInputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source and a sink for <a href="http://kafka.apache.org/">Kafka</a> topics.
 * Kafka version 0.8 are supported.
 */
public class Kafka8IO {
  /**
   * Static class, prevent instantiation.
   */
  private Kafka8IO() {
  }


  /**
   * Creates an uninitialized {@link Read} {@link PTransform}. Before use, basic Kafka
   * configuration should set with {@link Read#withBootstrapServers(String)} and
   * {@link Read#withTopics(List)}. Other optional settings include key and value coders,
   * custom timestamp and watermark functions.
   */
  public static Read<byte[], byte[]> readBytes() {
    return new AutoValue_Kafka8IO_Read.Builder<byte[], byte[]>()
            .setTopics(new ArrayList<String>())
            .setTopicPartitions(new ArrayList<TopicAndPartition>())
            .setKeyCoder(ByteArrayCoder.of())
            .setValueCoder(ByteArrayCoder.of())
            .setKafkaClusterFactoryFn(Read.KAFKA_CLUSTER_FACTORY_FN)
            .setConsumerConfig(Read.DEFAULT_CONSUMER_PROPERTIES)
            .setMaxNumRecords(Long.MAX_VALUE)
            .build();
  }

  /**
   * Creates an uninitialized {@link Read} {@link PTransform}. Before use, basic Kafka
   * configuration should set with {@link Read#withBootstrapServers(String)} and
   * {@link Read#withTopics(List)}. Other optional settings include key and value coders,
   * custom timestamp and watermark functions.
   */
  public static <K, V> Read<K, V> read() {
    return new AutoValue_Kafka8IO_Read.Builder<K, V>()
            .setTopics(new ArrayList<String>())
            .setTopicPartitions(new ArrayList<TopicAndPartition>())
            .setKafkaClusterFactoryFn(Read.KAFKA_CLUSTER_FACTORY_FN)
            .setConsumerConfig(Read.DEFAULT_CONSUMER_PROPERTIES)
            .setMaxNumRecords(Long.MAX_VALUE)
            .build();
  }

  /**
   * Creates an uninitialized {@link Write} {@link PTransform}. Before use, Kafka configuration
   * should be set with {@link Write#withBootstrapServers(String)} and {@link Write#withTopic}
   * along with {@link Coder}s for (optional) key and values.
   */
  public static <K, V> Write<K, V> write() {
    return new AutoValue_Kafka8IO_Write.Builder<K, V>()
            .setProducerConfig(Write.DEFAULT_PRODUCER_PROPERTIES)
            .setValueOnly(false)
            .build();
  }

  ///////////////////////// Read Support \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  /**
   * A {@link PTransform} to read from Kafka topics. See {@link Kafka8IO} for more information on
   * usage and configuration.
   */
  @AutoValue
  public abstract static class Read<K, V>
          extends PTransform<PBegin, PCollection<KafkaRecord<K, V>>> {
    abstract Map<String, String> getConsumerConfig();

    abstract List<String> getTopics();

    abstract List<TopicAndPartition> getTopicPartitions();

    @Nullable
    abstract Coder<K> getKeyCoder();

    @Nullable
    abstract Coder<V> getValueCoder();

    abstract SerializableFunction<Map<String, String>, KafkaCluster>
    getKafkaClusterFactoryFn();

    @Nullable
    abstract SerializableFunction<KafkaRecord<K, V>, Instant> getTimestampFn();

    @Nullable
    abstract SerializableFunction<KafkaRecord<K, V>, Instant> getWatermarkFn();

    abstract long getMaxNumRecords();

    @Nullable
    abstract Duration getMaxReadTime();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setConsumerConfig(Map<String, String> config);

      abstract Builder<K, V> setTopics(List<String> topics);

      abstract Builder<K, V> setTopicPartitions(List<TopicAndPartition> tps);

      abstract Builder<K, V> setKeyCoder(Coder<K> keyCoder);

      abstract Builder<K, V> setValueCoder(Coder<V> valueCoder);

      abstract Builder<K, V> setKafkaClusterFactoryFn(
              SerializableFunction<Map<String, String>, KafkaCluster> kafkaClusterFactoryFn);

      abstract Builder<K, V> setTimestampFn(SerializableFunction<KafkaRecord<K, V>, Instant> fn);

      abstract Builder<K, V> setWatermarkFn(SerializableFunction<KafkaRecord<K, V>, Instant> fn);

      abstract Builder<K, V> setMaxNumRecords(long maxNumRecords);

      abstract Builder<K, V> setMaxReadTime(Duration maxReadTime);

      abstract Read<K, V> build();
    }

    /**
     * Returns a new {@link Read} with Kafka SimpleConsumer pointing to {@code bootstrapServers}.
     */
    public Read<K, V> withBootstrapServers(String bootstrapServers) {
      return updateKafkaClusterProperties(
              ImmutableMap.of(
                      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Returns a new {@link Read} that reads from the topics. All the partitions from each
     * of the topics are read.
     * See {@link UnboundedKafkaSource#split(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopics(List<String> topics) {
      checkState(
          getTopicPartitions().isEmpty(), "Only topics or topicPartitions can be set, not both");
      return toBuilder().setTopics(ImmutableList.copyOf(topics)).build();
    }

    /**
     * Returns a new {@link Read} that reads from the partitions. This allows reading only a subset
     * of partitions for one or more topics when (if ever) needed.
     * See {@link UnboundedKafkaSource#split(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopicPartitions(List<TopicAndPartition> topicAndPartitions) {
      checkState(getTopics().isEmpty(), "Only topics or topicPartitions can be set, not both");
      return toBuilder().setTopicPartitions(ImmutableList.copyOf(topicAndPartitions)).build();
    }

    /**
     * Returns a new {@link Read} with {@link Coder} for key bytes.
     */
    public Read<K, V> withKeyCoder(Coder<K> keyCoder) {
      return toBuilder().setKeyCoder(keyCoder).build();
    }

    /**
     * Returns a new {@link Read} with {@link Coder} for value bytes.
     */
    public Read<K, V> withValueCoder(Coder<V> valueCoder) {
      return toBuilder().setValueCoder(valueCoder).build();
    }

    /**
     * A factory to create {@link KafkaCluster} from kafka configuration.
     * Default is {@link KafkaCluster}.
     */
    public Read<K, V> withKafkaClusterFactoryFn(
            SerializableFunction<Map<String, String>, KafkaCluster> kafkaClusterFactoryFn) {
      return toBuilder().setKafkaClusterFactoryFn(kafkaClusterFactoryFn).build();
    }

    /**
     * Update SimpleConsumer configuration with new properties.
     */
    public Read<K, V> updateKafkaClusterProperties(Map<String, String> configUpdates) {
      Map<String, String> config = updateKafkaProperties(getConsumerConfig(),
              IGNORED_CONSUMER_PROPERTIES, configUpdates);
      return toBuilder().setConsumerConfig(config).build();
    }

    /**
     * Similar to {@link org.apache.beam.sdk.io.Read.Unbounded#withMaxNumRecords(long)}.
     * Mainly used for tests and demo applications.
     */
    public Read<K, V> withMaxNumRecords(long maxNumRecords) {
      return toBuilder().setMaxNumRecords(maxNumRecords).setMaxReadTime(null).build();
    }

    /**
     * Similar to
     * {@link org.apache.beam.sdk.io.Read.Unbounded#withMaxReadTime(Duration)}.
     * Mainly used for tests and demo
     * applications.
     */
    public Read<K, V> withMaxReadTime(Duration maxReadTime) {
      return toBuilder().setMaxNumRecords(Long.MAX_VALUE).setMaxReadTime(maxReadTime).build();
    }

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     */
    public Read<K, V> withTimestampFn2(
            SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {
      checkNotNull(timestampFn);
      return toBuilder().setTimestampFn(timestampFn).build();
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp
     *
     * @see #withTimestampFn(SerializableFunction)
     */
    public Read<K, V> withWatermarkFn2(
            SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn) {
      checkNotNull(watermarkFn);
      return toBuilder().setWatermarkFn(watermarkFn).build();
    }

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     */
    public Read<K, V> withTimestampFn(SerializableFunction<KV<K, V>, Instant> timestampFn) {
      checkNotNull(timestampFn);
      return withTimestampFn2(unwrapKafkaAndThen(timestampFn));
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp
     *
     * @see #withTimestampFn(SerializableFunction)
     */
    public Read<K, V> withWatermarkFn(SerializableFunction<KV<K, V>, Instant> watermarkFn) {
      checkNotNull(watermarkFn);
      return withWatermarkFn2(unwrapKafkaAndThen(watermarkFn));
    }

    /**
     * Returns a {@link PTransform} for PCollection of {@link KV}, dropping Kafka metatdata.
     */
    public PTransform<PBegin, PCollection<KV<K, V>>> withoutMetadata() {
      return new TypedWithoutMetadata<K, V>(this);
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
              "Kafka bootstrap servers should be set");
      checkArgument(getTopics().size() > 0 || getTopicPartitions().size() > 0,
              "Kafka topics or topic_partitions are required");
      checkNotNull(getKeyCoder(), "Key coder must be set");
      checkNotNull(getValueCoder(), "Value coder must be set");
    }

    public PCollection<KafkaRecord<K, V>> expand(PBegin input) {
      // Handles unbounded source to bounded conversion if maxNumRecords or maxReadTime is set.
      Unbounded<KafkaRecord<K, V>> unbounded =
              org.apache.beam.sdk.io.Read.from(makeSource());

      PTransform<PBegin, PCollection<KafkaRecord<K, V>>> transform = unbounded;

      if (getMaxNumRecords() < Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(getMaxNumRecords());
      } else if (getMaxReadTime() != null) {
        transform = unbounded.withMaxReadTime(getMaxReadTime());
      }

      return input.getPipeline().apply(transform);
    }

    /**
     * Creates an {@link UnboundedSource UnboundedSource&lt;KafkaRecord&lt;K, V&gt;, ?&gt;} with the
     * configuration in {@link Read}. Primary use case is unit tests, should not be used in an
     * application.
     */
    @VisibleForTesting
    UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> makeSource() {
      return new UnboundedKafkaSource<K, V>(this, -1);
    }

    // utility method to convert KafkRecord<K, V> to user KV<K, V> before applying user functions
    private static <KeyT, ValueT, OutT> SerializableFunction<KafkaRecord<KeyT, ValueT>, OutT>
    unwrapKafkaAndThen(final SerializableFunction<KV<KeyT, ValueT>, OutT> fn) {
      return new SerializableFunction<KafkaRecord<KeyT, ValueT>, OutT>() {
        public OutT apply(KafkaRecord<KeyT, ValueT> record) {
          return fn.apply(record.getKV());
        }
      };
    }
    ///////////////////////////////////////////////////////////////////////////////////////

    /**
     * A set of properties that are not required or don't make sense for our SimpleConsumer.
     */
    private static final Map<String, String> IGNORED_CONSUMER_PROPERTIES = ImmutableMap.of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Set keyCoder instead",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "Set valueCoder instead"
            // "group.id", "enable.auto.commit", "auto.commit.interval.ms" :
            //     lets allow these, applications can have better resume point for restarts.
    );

    // set config defaults
    private static final Map<String, String> DEFAULT_CONSUMER_PROPERTIES = ImmutableMap.of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            // default to latest offset when we are not resuming.
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
            // disable auto commit of offsets. we don't require group_id. could be enabled by user.
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


    @SuppressWarnings("unchecked")
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      List<String> topics = getTopics();
      List<TopicAndPartition> tps = getTopicPartitions();
      if (topics.size() > 0) {
        builder.add(DisplayData.item("topics", Joiner.on(",").join(topics)).withLabel("Topic/s"));
      } else if (tps.size() > 0) {
        builder.add(DisplayData.item("topicPartitions", Joiner.on(",").join(tps))
                .withLabel("Topic Partition/s"));
      }
      Set<String> ignoredConsumerPropertiesKeys = IGNORED_CONSUMER_PROPERTIES.keySet();
      for (Map.Entry<String, String> conf : getConsumerConfig().entrySet()) {
        String key = conf.getKey();
        if (!ignoredConsumerPropertiesKeys.contains(key)) {
          builder.add(DisplayData.item(key, ValueProvider.StaticValueProvider.of(conf.getValue())));
        }
      }
    }

    private static final SerializableFunction<Map<String, String>, KafkaCluster>
            KAFKA_CLUSTER_FACTORY_FN =
            new SerializableFunction<Map<String, String>, KafkaCluster>() {
              public KafkaCluster apply(Map<String, String> config) {
                return new KafkaCluster(config);
              }
            };
  }

  private static final Logger LOG = LoggerFactory.getLogger(Kafka8IO.class);

  /**
   * Returns a new config map which is merge of current config and updates.
   * Verifies the updates do not includes ignored properties.
   */
  private static Map<String, String> updateKafkaProperties(
          Map<String, String> currentConfig,
          Map<String, String> ignoredProperties,
          Map<String, String> updates) {

    for (String key : updates.keySet()) {
      checkArgument(!ignoredProperties.containsKey(key),
              "No need to configure '%s'. %s", key, ignoredProperties.get(key));
    }

    Map<String, String> config = new HashMap<>(currentConfig);
    config.putAll(updates);

    return config;
  }

  /**
   * Returns a new config map which is merge of current config and updates.
   * Verifies the updates do not includes ignored properties.
   */
  private static Map<String, Object> updateKafkaProducerProperties(
          Map<String, Object> currentConfig,
          Map<String, String> ignoredProperties,
          Map<String, Object> updates) {

    for (String key : updates.keySet()) {
      checkArgument(!ignoredProperties.containsKey(key),
              "No need to configure '%s'. %s", key, ignoredProperties.get(key));
    }

    Map<String, Object> config = new HashMap<>(currentConfig);
    config.putAll(updates);

    return config;
  }

  /**
   * A {@link PTransform} to read from Kafka topics. Similar to {@link Kafka8IO.Read}, but
   * removes Kafka metatdata and returns a {@link PCollection} of {@link KV}.
   * See {@link Kafka8IO} for more information on usage and configuration of reader.
   */
  public static class TypedWithoutMetadata<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {
    private final Read<K, V> read;

    TypedWithoutMetadata(Read<K, V> read) {
      super("KafkaIO.Read");
      this.read = read;
    }

    @Override
    public PCollection<KV<K, V>> expand(PBegin begin) {
      return read
              .expand(begin)
              .apply("Remove Kafka Metadata",
                      ParDo.of(new DoFn<KafkaRecord<K, V>, KV<K, V>>() {
                        @ProcessElement
                        public void processElement(ProcessContext ctx) {
                          ctx.output(ctx.element().getKV());
                        }
                      }));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      read.populateDisplayData(builder);
    }
  }

  private static class UnboundedKafkaSource<K, V>
          extends UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> {
    private Read<K, V> spec;
    private final int id; // split id, mainly for debugging

    public UnboundedKafkaSource(Read<K, V> spec, int id) {
      this.spec = spec;
      this.id = id;
    }

    /**
     * The partitions are evenly distributed among the splits. The number of splits returned is
     * {@code min(desiredNumSplits, totalNumPartitions)}, though better not to depend on the exact
     * count.
     *
     * <p>It is important to assign the partitions deterministically so that we can support
     * resuming a split from last checkpoint. The Kafka partitions are sorted by
     * {@code <topic, partition>} and then assigned to splits in round-robin order.
     */
    @Override
    public List<UnboundedKafkaSource<K, V>> split(
            int desiredNumSplits, PipelineOptions options) throws Exception {

      List<TopicAndPartition> partitions = new ArrayList<>(spec.getTopicPartitions());

      // (a) fetch partitions for each topic
      // (b) sort by <topic, partition>
      // (c) round-robin assign the partitions to splits

      if (partitions.isEmpty()) {
        KafkaCluster kafkaCluster =
                spec.getKafkaClusterFactoryFn().apply(spec.getConsumerConfig());
        partitions.addAll(kafkaCluster.getPartitions(spec.getTopics()));
      }

      Collections.sort(partitions, new Comparator<TopicAndPartition>() {
        public int compare(TopicAndPartition tp1, TopicAndPartition tp2) {
          return ComparisonChain
                  .start()
                  .compare(tp1.topic(), tp2.topic())
                  .compare(tp1.partition(), tp2.partition())
                  .result();
        }
      });

      checkArgument(desiredNumSplits > 0);
      checkState(partitions.size() > 0,
              "Could not find any partitions. Please check Kafka configuration and topic names");

      int numSplits = Math.min(desiredNumSplits, partitions.size());
      List<List<TopicAndPartition>> assignments = new ArrayList<>(numSplits);

      for (int i = 0; i < numSplits; i++) {
        assignments.add(new ArrayList<TopicAndPartition>());
      }
      for (int i = 0; i < partitions.size(); i++) {
        assignments.get(i % numSplits).add(partitions.get(i));
      }

      List<UnboundedKafkaSource<K, V>> result = new ArrayList<>(numSplits);

      for (int i = 0; i < numSplits; i++) {
        List<TopicAndPartition> assignedToSplit = assignments.get(i);

        LOG.info("Partitions assigned to split {} (total {}): {}",
                i, assignedToSplit.size(), Joiner.on(",").join(assignedToSplit));

        result.add(
                new UnboundedKafkaSource<>(
                        spec.toBuilder()
                                .setTopics(Collections.<String>emptyList())
                                .setTopicPartitions(assignedToSplit)
                                .build(),
                        i));
      }

      return result;
    }

    @Override
    public UnboundedKafkaReader<K, V> createReader(PipelineOptions options,
                                                   KafkaCheckpointMark checkpointMark) {
      if (spec.getTopicPartitions().isEmpty()) {
        LOG.warn("Looks like generateSplits() is not called. Generate single split.");
        try {
          return new UnboundedKafkaReader<K, V>(
              split(1, options).get(0), checkpointMark);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return new UnboundedKafkaReader<K, V>(this, checkpointMark);
    }

    @Override
    public Coder<KafkaCheckpointMark> getCheckpointMarkCoder() {
      return AvroCoder.of(KafkaCheckpointMark.class);
    }

    @Override
    public boolean requiresDeduping() {
      // Kafka records are ordered with in partitions. In addition checkpoint guarantees
      // records are not consumed twice.
      return false;
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<KafkaRecord<K, V>> getDefaultOutputCoder() {
      return KafkaRecordCoder.of(spec.getKeyCoder(), spec.getValueCoder());
    }
  }

  private static class UnboundedKafkaReader<K, V> extends UnboundedReader<KafkaRecord<K, V>> {

    private static final int RETRIES = 3;
    private final UnboundedKafkaSource<K, V> source;
    private final String name;
    private final List<PartitionState> partitionStates;
    private KafkaRecord<K, V> curRecord;
    private Instant curTimestamp;
    private Iterator<PartitionState> curBatch = Collections.emptyIterator();

    private static final Duration KAFKA_POLL_TIMEOUT = Duration.millis(1000);
    private static final Duration NEW_RECORDS_POLL_TIMEOUT = Duration.millis(10);

    private final SynchronousQueue<KafkaMessage> availableRecordsQueue =
            new SynchronousQueue<>();
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final ExecutorService consumerFetchThread;
    private final ScheduledExecutorService offsetFetcherThread =
            Executors.newSingleThreadScheduledExecutor();
    private static final int OFFSET_UPDATE_INTERVAL_SECONDS = 5;

    private static final long UNINITIALIZED_OFFSET = -1;


    /**
     * watermark before any records have been read.
     */
    private static Instant initialWatermark = new Instant(Long.MIN_VALUE);

    public String toString() {
      return name;
    }

    // maintains state of each assigned partition (buffered records, consumed offset, etc)
    private static class PartitionState {
      private final TopicAndPartition topicPartition;
      private long nextOffset;
      private long latestOffset;
      private Iterator<MessageAndOffset> recordIter = Collections.emptyIterator();
      private KafkaCluster kc;
      private SimpleConsumer simpleConsumer;

      // simple moving average for size of each record in bytes
      private double avgRecordSize = 0;
      private static final int movingAvgWindow = 1000; // very roughly avg of last 1000 elements

      PartitionState(TopicAndPartition partition, long nextOffset, KafkaCluster kafkaCluster) {
        this.topicPartition = partition;
        this.nextOffset = nextOffset;
        this.latestOffset = UNINITIALIZED_OFFSET;
        this.kc = kafkaCluster;
        this.simpleConsumer = connectLeader();
      }

      String topic() {
        return this.topicPartition.topic();
      }

      int partition() {
        return this.topicPartition.partition();
      }

      private Map<TopicAndPartition, KafkaCluster.LeaderOffset>
      latestLeaderOffsets(int retries, String reset) throws IOException {
        Map<TopicAndPartition, KafkaCluster.LeaderOffset> result;
        if ("smallest".equals(reset)) {
          result = kc.getEarliestLeaderOffsets(Sets.newHashSet(topicPartition));
        } else {
          result = kc.getLatestLeaderOffsets(Sets.newHashSet(topicPartition));
        }
        if (result.isEmpty()) {
          if (retries <= 0) {
            throw new IOException("Couldn't find leader offsets for " + topicPartition);
          } else {
            try {
              Thread.sleep(kc.config().refreshLeaderBackoffMs());
            } catch (InterruptedException e) {
              LOG.error("refreshLeaderBackoffMs throw InterruptedException", e);
            }
            return latestLeaderOffsets(retries - 1, reset);
          }
        } else {
          return result;
        }
      }


      long getLatestOffset(int retries) throws IOException {
        String reset = kc.config().autoOffsetReset().toLowerCase();
        return latestLeaderOffsets(retries, reset).get(this.topicPartition).getOffset();
      }


      SimpleConsumer getSimpleConsumer() {
        closeConsumer();
        return connectLeader();
      }

      void closeConsumer() {
        if (simpleConsumer != null) {
          simpleConsumer.close();
        }
      }

      private SimpleConsumer connectLeader() {
        return kc.connectLeader(topic(), partition());
      }


      // update consumedOffset and avgRecordSize
      void recordConsumed(long offset, int size) {
        nextOffset = offset + 1;

        // this is always updated from single thread. probably not worth making it an AtomicDouble
        if (avgRecordSize <= 0) {
          avgRecordSize = size;
        } else {
          // initially, first record heavily contributes to average.
          avgRecordSize += ((size - avgRecordSize) / movingAvgWindow);
        }
      }

      synchronized void setLatestOffset(long latestOffset) {
        this.latestOffset = latestOffset;
      }

      synchronized long approxBacklogInBytes() {
        // Note that is an an estimate of uncompressed backlog.
        if (latestOffset < 0 || nextOffset < 0) {
          return UnboundedReader.BACKLOG_UNKNOWN;
        }
        return Math.max(0, (long) ((latestOffset - nextOffset) * avgRecordSize));
      }


    }

    public UnboundedKafkaReader(
            UnboundedKafkaSource<K, V> source,
            @Nullable KafkaCheckpointMark checkpointMark) {

      this.source = source;
      this.name = "Reader-" + source.id;

      List<TopicAndPartition> partitions = source.spec.getTopicPartitions();
      final KafkaCluster kafkaCluster = source.spec.getKafkaClusterFactoryFn()
              .apply(source.spec.getConsumerConfig());
      partitionStates = ImmutableList.copyOf(Lists.transform(partitions,
              new Function<TopicAndPartition, PartitionState>() {
                public PartitionState apply(TopicAndPartition tp) {
                  return new PartitionState(tp, UNINITIALIZED_OFFSET, kafkaCluster);
                }
              }));
      this.consumerFetchThread = Executors.newFixedThreadPool(partitionStates.size());

      if (checkpointMark != null) {
        // a) verify that assigned and check-pointed partitions match exactly
        // b) set consumed offsets

        checkState(checkpointMark.getPartitions().size() == partitions.size(),
                "checkPointMark and assignedPartitions should match");
        // we could consider allowing a mismatch, though it is not expected in current Dataflow

        for (int i = 0; i < partitions.size(); i++) {
          PartitionMark ckptMark = checkpointMark.getPartitions().get(i);
          TopicAndPartition assigned = partitions.get(i);
          TopicAndPartition partition = new TopicAndPartition(ckptMark.getTopic(),
                  ckptMark.getPartition());
          checkState(partition.equals(assigned),
                  "checkpointed partition %s and assigned partition %s don't match",
                  partition, assigned);

          partitionStates.get(i).nextOffset = ckptMark.getNextOffset();
        }
      }
    }

    private void nextBatch() {
      curBatch = Collections.emptyIterator();

      KafkaMessage messages;
      try {
        // poll available records, wait (if necessary) up to the specified timeout.
        messages = availableRecordsQueue.poll(NEW_RECORDS_POLL_TIMEOUT.getMillis(),
                TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("{}: Unexpected", this, e);
        return;
      }

      if (messages == null) {
        return;
      }

      List<PartitionState> nonEmpty = new LinkedList<>();

      for (PartitionState p : partitionStates) {
        if (messages.getTopic().equals(p.topic()) && messages.getPartition() == p.partition()) {
          p.recordIter = messages.getByteBufferMessageSet().iterator();
        }
        if (p.recordIter.hasNext()) {
          nonEmpty.add(p);
        }
      }

      // cycle through the partitions in order to interleave records from each.
      curBatch = Iterators.cycle(nonEmpty);
    }

    private void handleFetchErr(FetchResponse resp, PartitionState p) throws InterruptedException {
      if (resp.hasError()) {
        short err = resp.errorCode(p.topic(), p.partition());
        if (err == ErrorMapping.LeaderNotAvailableCode()
                || err == ErrorMapping.NotLeaderForPartitionCode()) {
          LOG.error("Lost leader for topic " + p.topic() + " partition " + p.partition() + ", "
                  + " sleeping for " + p.kc.config().refreshLeaderBackoffMs() + " ms");
          Thread.sleep(p.kc.config().refreshLeaderBackoffMs());
        }
        throw new InterruptedException("errorCode " + err);
      }
    }


    private void fetchMessage(PartitionState p) {
      // Read in a loop and enqueue the batch of records, if any, to availableRecordsQueue
      while (!closed.get()) {
        try {
          FetchRequest req = new FetchRequestBuilder()
                  .addFetch(p.topic()
                          , p.partition()
                          , p.nextOffset
                          , p.kc.config().fetchMessageMaxBytes())
                  .build();

          FetchResponse resp = p.getSimpleConsumer().fetch(req);
          handleFetchErr(resp, p);
          if (!closed.get()) {
            KafkaMessage msg = new KafkaMessage(p.topic()
                    , p.partition()
                    , resp.messageSet(p.topic(), p.partition()));
            availableRecordsQueue.put(msg);
          }

        } catch (InterruptedException e) {
          LOG.warn("{}: consumer thread is interrupted", this, e); // not expected
          break;
        }
      }

      LOG.info("{}: Returning from consumer pool loop", this);
    }

    @Override
    public boolean start() throws IOException {
      for (final PartitionState p : partitionStates) {
        if (p.nextOffset != UNINITIALIZED_OFFSET) {
          // consumer.seek(p.topicPartition, p.nextOffset);
        } else {
          p.nextOffset = p.getLatestOffset(RETRIES);
        }
        consumerFetchThread.submit(
                new Runnable() {
                  public void run() {
                    fetchMessage(p);
                  }
                });
        LOG.info("{}: reading from {} starting at offset {}", name, p.topicPartition, p.nextOffset);
      }
      offsetFetcherThread.scheduleAtFixedRate(
              new Runnable() {
                public void run() {
                  updateLatestOffsets();
                }
              }, 0, OFFSET_UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);
      nextBatch();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      /* Read first record (if any). we need to loop here because :
       *  - (a) some records initially need to be skipped if they are before consumedOffset
       *  - (b) if curBatch is empty, we want to fetch next batch and then advance.
       *  - (c) curBatch is an iterator of iterators. we interleave the records from each.
       *        curBatch.next() might return an empty iterator.
       */
      while (true) {
        if (curBatch.hasNext()) {
          PartitionState pState = curBatch.next();

          if (!pState.recordIter.hasNext()) { // -- (c)
            pState.recordIter = Collections.emptyIterator(); // drop ref
            curBatch.remove();
            continue;
          }

          MessageAndOffset rawRecord = pState.recordIter.next();
          long expected = pState.nextOffset;
          long offset = rawRecord.offset();

          if (offset < expected) { // -- (a)
            // this can happen when compression is enabled in Kafka (seems to be fixed in 0.10)
            // should we check if the offset is way off from consumedOffset (say > 1M)?
            LOG.warn("{}: ignoring already consumed offset {} for {}",
                    this, offset, pState.topicPartition);
            continue;
          }

          // sanity check
          if (offset != expected) {
            LOG.warn("{}: gap in offsets for {} at {}. {} records missing.",
                    this, pState.topicPartition, expected, offset - expected);
          }

          if (curRecord == null) {
            LOG.info("{}: first record offset {}", name, offset);
          }

          curRecord = null; // user coders below might throw.

          // apply user coders. might want to allow skipping records that fail to decode.
          ByteBuffer payload = rawRecord.message().payload();
          byte[] messageBytes = null;
          if (payload != null) {
            messageBytes = new byte[payload.limit()];
            payload.get(messageBytes);
          }
          ByteBuffer key = rawRecord.message().key();
          byte[] keyBytes = null;
          if (key != null) {
            keyBytes = new byte[key.limit()];
            key.get(keyBytes);
          }

          // TODO: wrap exceptions from coders to make explicit to users
          KafkaRecord<K, V> record = new KafkaRecord<K, V>(
                  pState.topic(),
                  pState.partition(),
                  rawRecord.offset(),
                  System.currentTimeMillis(),
                  decode(keyBytes, source.spec.getKeyCoder()),
                  decode(messageBytes, source.spec.getValueCoder()));

          curTimestamp = (source.spec.getTimestampFn() == null)
                  ? Instant.now() : source.spec.getTimestampFn().apply(record);
          curRecord = record;

          int recordSize = (keyBytes == null ? 0 : keyBytes.length)
                  + (messageBytes == null ? 0 : messageBytes.length);
          pState.recordConsumed(offset, recordSize);
          return true;

        } else { // -- (b)
          nextBatch();

          if (!curBatch.hasNext()) {
            return false;
          }
        }
      }
    }

    private static byte[] nullBytes = new byte[0];

    private static <T> T decode(byte[] bytes, Coder<T> coder) throws IOException {
      // If 'bytes' is null, use byte[0]. It is common for key in Kakfa record to be null.
      // This makes it impossible for user to distinguish between zero length byte and null.
      // Alternately, we could have a ByteArrayCoder that handles nulls, and use that for default
      // coder.
      byte[] toDecode = bytes == null ? nullBytes : bytes;
      return coder.decode(new ExposedByteArrayInputStream(toDecode), Coder.Context.OUTER);
    }

    // update latest offset for each partition.
    // called from offsetFetcher thread
    private void updateLatestOffsets() {
      for (PartitionState p : partitionStates) {
        try {
          long offset = p.getLatestOffset(RETRIES);
          p.setLatestOffset(offset);
        } catch (Exception e) {
          // An exception is expected if we've closed the reader in another thread. Ignore and exit.
          if (closed.get()) {
            break;
          }
          LOG.warn("{}: exception while fetching latest offset for partition {}. will be retried.",
                  this, p.topicPartition, e);
          p.setLatestOffset(UNINITIALIZED_OFFSET); // reset
        }

        LOG.debug("{}: latest offset update for {} : {} (consumer offset {}, avg record size {})",
                this, p.topicPartition, p.latestOffset, p.nextOffset, p.avgRecordSize);
      }

      LOG.debug("{}:  backlog {}", this, getSplitBacklogBytes());
    }

    @Override
    public Instant getWatermark() {
      if (curRecord == null) {
        LOG.debug("{}: getWatermark() : no records have been read yet.", name);
        return initialWatermark;
      }

      return source.spec.getWatermarkFn() != null
              ? source.spec.getWatermarkFn().apply(curRecord) : curTimestamp;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new KafkaCheckpointMark(ImmutableList.copyOf(// avoid lazy (consumedOffset can change)
              Lists.transform(partitionStates,
                      new Function<PartitionState, PartitionMark>() {
                        public PartitionMark apply(PartitionState p) {
                          return new PartitionMark(p.topicPartition.topic(),
                                  p.topicPartition.partition(),
                                  p.nextOffset);
                        }
                      }
              )));
    }

    @Override
    public UnboundedSource<KafkaRecord<K, V>, ?> getCurrentSource() {
      return source;
    }

    @Override
    public KafkaRecord<K, V> getCurrent() throws NoSuchElementException {
      // should we delay updating consumed offset till this point? Mostly not required.
      return curRecord;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return curTimestamp;
    }


    @Override
    public long getSplitBacklogBytes() {
      long backlogBytes = 0;

      for (PartitionState p : partitionStates) {
        long pBacklog = p.approxBacklogInBytes();
        if (pBacklog == UnboundedReader.BACKLOG_UNKNOWN) {
          return UnboundedReader.BACKLOG_UNKNOWN;
        }
        backlogBytes += pBacklog;
      }

      return backlogBytes;
    }

    @Override
    public void close() throws IOException {
      closed.set(true);

      consumerFetchThread.shutdown();
      offsetFetcherThread.shutdown();

      boolean isShutdown = false;

      // Wait for threads to shutdown. Trying this as a loop to handle a tiny race where poll thread
      // might block to enqueue right after availableRecordsQueue.poll() below.
      while (!isShutdown) {

        availableRecordsQueue.poll(); // drain unread batch, this unblocks consumer thread.
        try {
          isShutdown = consumerFetchThread.awaitTermination(10, TimeUnit.SECONDS)
                  && offsetFetcherThread.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e); // not expected
        }

        if (!isShutdown) {
          LOG.warn("An internal thread is taking a long time to shutdown. will retry.");
        }
      }
      for (PartitionState p : partitionStates) {
        p.closeConsumer();
      }
    }
  }

  //////////////////////// Sink Support \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  /**
   * A {@link PTransform} to write to a Kafka topic. See {@link Kafka8IO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Write<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {
    @Nullable abstract String getTopic();
    @Nullable abstract Coder<K> getKeyCoder();
    @Nullable abstract Coder<V> getValueCoder();
    abstract boolean getValueOnly();
    abstract Map<String, Object> getProducerConfig();
    @Nullable
    abstract SerializableFunction<Map<String, Object>, Producer<K, V>> getProducerFactoryFn();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setTopic(String topic);
      abstract Builder<K, V> setKeyCoder(Coder<K> keyCoder);
      abstract Builder<K, V> setValueCoder(Coder<V> valueCoder);
      abstract Builder<K, V> setValueOnly(boolean valueOnly);
      abstract Builder<K, V> setProducerConfig(Map<String, Object> producerConfig);
      abstract Builder<K, V> setProducerFactoryFn(
              SerializableFunction<Map<String, Object>, Producer<K, V>> fn);
      abstract Write<K, V> build();
    }

    /**
     * Returns a new {@link Write} transform with Kafka producer pointing to
     * {@code bootstrapServers}.
     */
    public Write<K, V> withBootstrapServers(String bootstrapServers) {
      return updateProducerProperties(
              ImmutableMap.<String, Object>of(
                      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Returns a new {@link Write} transform that writes to given topic.
     */
    public Write<K, V> withTopic(String topic) {
      return toBuilder().setTopic(topic).build();
    }

    /**
     * Returns a new {@link Write} with {@link Coder} for serializing key (if any) to bytes.
     * A key is optional while writing to Kafka. Note when a key is set, its hash is used to
     * determine partition in Kafka (see {@link ProducerRecord} for more details).
     */
    public Write<K, V> withKeyCoder(Coder<K> keyCoder) {
      return toBuilder().setKeyCoder(keyCoder).build();
    }

    /**
     * Returns a new {@link Write} with {@link Coder} for serializing value to bytes.
     */
    public Write<K, V> withValueCoder(Coder<V> valueCoder) {
      return toBuilder().setValueCoder(valueCoder).build();
    }

    public Write<K, V> updateProducerProperties(Map<String, Object> configUpdates) {
      Map<String, Object> config = updateKafkaProducerProperties(getProducerConfig(),
              IGNORED_PRODUCER_PROPERTIES, configUpdates);
      return toBuilder().setProducerConfig(config).build();
    }

    /**
     * Returns a new {@link Write} with a custom function to create Kafka producer. Primarily used
     * for tests. Default is {@link KafkaProducer}
     */
    public Write<K, V> withProducerFactoryFn(
            SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn) {
      return toBuilder().setProducerFactoryFn(producerFactoryFn).build();
    }

    /**
     * Returns a new transform that writes just the values to Kafka. This is useful for writing
     * collections of values rather thank {@link KV}s.
     */
    public PTransform<PCollection<V>, PDone> values() {
      return new KafkaValueWrite<>(toBuilder().setValueOnly(true).build());
    }

    @Override
    public PDone expand(PCollection<KV<K, V>> input) {
      input.apply(ParDo.of(new KafkaWriter<>(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(getProducerConfig().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
              "Kafka bootstrap servers should be set");
      checkNotNull(getTopic(), "Kafka topic should be set");
      if (!getValueOnly()) {
        checkNotNull(getKeyCoder(), "Key coder should be set");
      }
      checkNotNull(getValueCoder(), "Value coder should be set");
    }

    // set config defaults
    private static final Map<String, Object> DEFAULT_PRODUCER_PROPERTIES =
            ImmutableMap.<String, Object>of(
                    ProducerConfig.RETRIES_CONFIG, 3,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, CoderBasedKafkaSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CoderBasedKafkaSerializer.class);

    /**
     * A set of properties that are not required or don't make sense for our consumer.
     */
    private static final Map<String, String> IGNORED_PRODUCER_PROPERTIES = ImmutableMap.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "Set keyCoder instead",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "Set valueCoder instead",
            configForKeySerializer(), "Reserved for internal serializer",
            configForValueSerializer(), "Reserved for internal serializer"
    );

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("topic", getTopic()).withLabel("Topic"));
      Set<String> ignoredProducerPropertiesKeys = IGNORED_PRODUCER_PROPERTIES.keySet();
      for (Map.Entry<String, Object> conf : getProducerConfig().entrySet()) {
        String key = conf.getKey();
        if (!ignoredProducerPropertiesKeys.contains(key)) {
          builder.add(DisplayData.item(key, ValueProvider.StaticValueProvider.of(conf.getValue())));
        }
      }
    }
  }

  /**
   * Same as {@code Write<K, V>} without a Key. Null is used for key as it is the convention is
   * Kafka when there is no key specified. Majority of Kafka writers don't specify a key.
   */
  private static class KafkaValueWrite<K, V> extends PTransform<PCollection<V>, PDone> {
    private final Write<K, V> kvWriteTransform;

    private KafkaValueWrite(Write<K, V> kvWriteTransform) {
      this.kvWriteTransform = kvWriteTransform;
    }

    @Override
    public PDone expand(PCollection<V> input) {
      return input
              .apply("Kafka values with default key",
                      MapElements.via(new SimpleFunction<V, KV<K, V>>() {
                        @Override
                        public KV<K, V> apply(V element) {
                          return KV.of(null, element);
                        }
                      }))
              .setCoder(KvCoder.of(new NullOnlyCoder<K>(), kvWriteTransform.getValueCoder()))
              .apply(kvWriteTransform);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      kvWriteTransform.populateDisplayData(builder);
    }
  }

  private static class NullOnlyCoder<T> extends AtomicCoder<T> {
    @Override
    public void encode(T value, OutputStream outStream) {
      checkArgument(value == null, "Can only encode nulls");
      // Encode as no bytes.
    }

    @Override
    public T decode(InputStream inStream) {
      return null;
    }
  }

  private static class KafkaWriter<K, V> extends DoFn<KV<K, V>, Void> {

    @Setup
    public void setup() {
      if (spec.getProducerFactoryFn() != null) {
        producer = spec.getProducerFactoryFn().apply(producerConfig);
      } else {
        producer = new KafkaProducer<K, V>(producerConfig);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) throws Exception {
      checkForFailures();

      KV<K, V> kv = ctx.element();
      producer.send(
              new ProducerRecord<K, V>(spec.getTopic(), kv.getKey(), kv.getValue()),
              new SendCallback());
    }

    @FinishBundle
    public void finishBundle() throws IOException {
      checkForFailures();
    }

    @Teardown
    public void teardown() {
      producer.close();
    }

    ///////////////////////////////////////////////////////////////////////////////////

    private final Write<K, V> spec;
    private final Map<String, Object> producerConfig;

    private transient Producer<K, V> producer = null;
    //private transient Callback sendCallback = new SendCallback();
    // first exception and number of failures since last invocation of checkForFailures():
    private transient Exception sendException = null;
    private transient long numSendFailures = 0;

    KafkaWriter(Write<K, V> spec) {
      this.spec = spec;

      // Set custom kafka serializers. We can not serialize user objects then pass the bytes to
      // producer. The key and value objects are used in kafka Partitioner interface.
      // This does not matter for default partitioner in Kafka as it uses just the serialized
      // key bytes to pick a partition. But are making sure user's custom partitioner would work
      // as expected.

      this.producerConfig = new HashMap<>(spec.getProducerConfig());
      this.producerConfig.put(configForKeySerializer(), spec.getKeyCoder());
      this.producerConfig.put(configForValueSerializer(), spec.getValueCoder());
    }

    private synchronized void checkForFailures() throws IOException {
      if (numSendFailures == 0) {
        return;
      }

      String msg = String.format(
              "KafkaWriter : failed to send %d records (since last report)", numSendFailures);

      Exception e = sendException;
      sendException = null;
      numSendFailures = 0;

      LOG.warn(msg);
      throw new IOException(msg, e);
    }

    private class SendCallback implements Callback {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
          return;
        }

        synchronized (KafkaWriter.this) {
          if (sendException == null) {
            sendException = exception;
          }
          numSendFailures++;
        }
        // don't log exception stacktrace here, exception will be propagated up.
        LOG.warn("KafkaWriter send failed : '{}'", exception.getMessage());
      }
    }
  }

  /**
   * Implements Kafka's {@link Serializer} with a {@link Coder}. The coder is stored as serialized
   * value in producer configuration map.
   */
  public static class CoderBasedKafkaSerializer<T> implements Serializer<T> {

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      String configKey = isKey ? configForKeySerializer() : configForValueSerializer();
      coder = (Coder<T>) configs.get(configKey);
      checkNotNull(coder, "could not instantiate coder for Kafka serialization");
    }

    @Override
    public byte[] serialize(String topic, @Nullable T data) {
      if (data == null) {
        return null; // common for keys to be null
      }

      try {
        return CoderUtils.encodeToByteArray(coder, data);
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
    }

    private Coder<T> coder = null;
    private static final String CONFIG_FORMAT = "beam.coder.based.kafka8.%s.serializer";
  }


  private static String configForKeySerializer() {
    return String.format(CoderBasedKafkaSerializer.CONFIG_FORMAT, "key");
  }

  private static String configForValueSerializer() {
    return String.format(CoderBasedKafkaSerializer.CONFIG_FORMAT, "value");
  }
}
