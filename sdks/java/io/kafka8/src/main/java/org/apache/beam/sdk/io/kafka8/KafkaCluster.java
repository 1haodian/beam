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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;


import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;

import kafka.javaapi.consumer.SimpleConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class KafkaCluster implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaCluster.class);

  private Map<String, Integer> seedBrokers = new HashMap<>();
  private Properties props;

  private SimpleConsumerConfig config = null;


  KafkaCluster(Map<String, String> kafkaParams) {
    String brokers = kafkaParams.get("metadata.broker.list");
    if (brokers == null) {
      brokers = kafkaParams.get("bootstrap.servers");
    }
    checkNotNull(brokers, "Must specify metadata.broker.list or bootstrap.servers");


    Map<String, String> filtered = Maps.filterKeys(kafkaParams, new Predicate<String>() {
      @Override
      public boolean apply(@Nonnull String key) {
        return !key.equals("metadata.broker.list") && !key.equals("bootstrap.servers");
      }
    });
    Properties props = new Properties();
    Set<String> keys = filtered.keySet();
    for (String key : keys) {
      props.put(key, filtered.get(key));
    }
    List<String> configs = Arrays.asList("zookeeper.connect", "group.id");
    for (String config : configs) {
      if (!props.containsKey(config)) {
        props.setProperty(config, "");
      }
    }
    this.props = props;

    String[] brokerWithPort = brokers.split(",");
    for (String eachBrocker : brokerWithPort) {
      String[] hpa = eachBrocker.split(":");
      checkState(hpa.length != 1,
              "Broker not in the correct format of <host>:<port> [$brokers]");
      this.seedBrokers.put(hpa[0], Integer.valueOf(hpa[1]));
    }
  }


  public synchronized SimpleConsumerConfig config() {
    if (config == null) {
      config = new SimpleConsumerConfig(this.seedBrokers, this.props);
    }
    return config;
  }

  SimpleConsumer connect(String host, int port) {
    return new SimpleConsumer(host, port, config().socketTimeoutMs(),
            config().socketReceiveBufferBytes(), config().clientId());
  }

  SimpleConsumer connectLeader(String topic, int partition) {
    Leader leader = findLeader(topic, partition);
    return connect(leader.getHost(), leader.getPort());
  }

  Leader findLeader(String topic, int partition) {

    Map<String, Integer> brokers = config().getSeedBrokers();
    List<String> hosts = Lists.newArrayList(brokers.keySet());
    Collections.shuffle(hosts);
    PartitionMetadata returnMetaData = null;
    TopicMetadataRequest req = new TopicMetadataRequest(
            kafka.api.TopicMetadataRequest.CurrentVersion(),
            0, config().clientId(), Collections.singletonList(topic));
    loop:
    for (String host : hosts) {
      SimpleConsumer consumer = null;
      try {
        int port = brokers.get(host);
        consumer = connect(host, port);
        TopicMetadataResponse resp = consumer.send(req);
        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          if (item.topic().equals(topic)) {
            for (PartitionMetadata part : item.partitionsMetadata()) {
              if (part.partitionId() == partition) {
                returnMetaData = part;
                break loop;
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Error communicating with Broker [" + host + "] to find Leader for "
                + "[" + topic + ", " + partition + "] Reason: ", e);
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }

    return new Leader(returnMetaData.leader().host(), returnMetaData.leader().port());
  }

  Map<TopicAndPartition, Leader> findLeaders(Set<TopicAndPartition> topicAndPartitions) {
    Map<TopicAndPartition, Leader> result = new HashMap<>();
    List<String> topics = Lists.newArrayList();
    for (TopicAndPartition eachTopicAndPartition : topicAndPartitions) {
      topics.add(eachTopicAndPartition.topic());
    }
    Set<TopicMetadata> topicMetadatas = getPartitionMetadata(new HashSet<>(topics));
    for (TopicMetadata tm : topicMetadatas) {
      List<PartitionMetadata> partitionMetadatas = tm.partitionsMetadata();
      for (PartitionMetadata pm : partitionMetadatas) {
        TopicAndPartition tp = new TopicAndPartition(tm.topic(), pm.partitionId());
        if (topicAndPartitions.contains(tp)) {
          String host = pm.leader().host();
          int port = pm.leader().port();
          result.put(tp, new Leader(host, port));
        }
      }
    }
    Set<TopicAndPartition> missing = Sets.difference(topicAndPartitions, result.keySet());
    checkState(result.keySet().size() == topicAndPartitions.size(),
            "Couldn't find leaders for " + missing);
    return result;
  }


  Set<TopicMetadata> getPartitionMetadata(Set<String> topics) {

    Set<TopicMetadata> result = null;
    TopicMetadataRequest req = new TopicMetadataRequest(
            kafka.api.TopicMetadataRequest.CurrentVersion(),
            0, config().clientId(), Lists.newArrayList(topics));
    Map<String, Integer> brokers = config().getSeedBrokers();
    List<String> hosts = Lists.newArrayList(brokers.keySet());
    Collections.shuffle(hosts);
    for (String host : hosts) {
      SimpleConsumer consumer = null;
      try {
        int port = brokers.get(host);
        consumer = connect(host, port);
        TopicMetadataResponse resp = consumer.send(req);
        List<TopicMetadata> metaDatas = resp.topicsMetadata();
        List<TopicMetadata> errs = Lists.newArrayList();
        for (TopicMetadata metaData : metaDatas) {
          if (metaData.errorCode() != ErrorMapping.NoError()) {
            errs.add(metaData);
          }
        }
        if (errs.isEmpty()) {
          result = new HashSet<>(metaDatas);
          break;
        }

      } catch (Exception e) {
        LOG.error("Error communicating with Broker [" + host + "] to find Leader for ["
                + topics + "] Reason: ", e);
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }
    return result;
  }

  List<TopicAndPartition> getPartitions(List<String> topics) {
    List<TopicAndPartition> result = Lists.newArrayList();
    Set<TopicMetadata> topicMetadata = getPartitionMetadata(new HashSet<>(topics));
    for (TopicMetadata tm : topicMetadata) {
      List<PartitionMetadata> partitionMetadatas = tm.partitionsMetadata();
      for (PartitionMetadata pm : partitionMetadatas) {
        result.add(new TopicAndPartition(tm.topic(), pm.partitionId()));
      }
    }
    return result;
  }

  public class LeaderOffset {
    private String host;
    private int port;
    private long offset;

    LeaderOffset(String host, int port, long offset) {
      this.host = host;
      this.port = port;
      this.offset = offset;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    long getOffset() {
      return offset;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LeaderOffset that = (LeaderOffset) o;
      return port == that.port
              && offset == that.offset
              && com.google.common.base.Objects.equal(host, that.host);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(host, port, offset);
    }
  }

  public class Leader {
    private String host;
    private int port;

    Leader(String host, int port) {
      this.host = host;
      this.port = port;
    }

    String getHost() {
      return host;
    }

    int getPort() {
      return port;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Leader leader = (Leader) o;
      return port == leader.port
              && Objects.equal(host, leader.host);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(host, port);
    }
  }

  Map<TopicAndPartition, LeaderOffset> getLeaderOffsets(Set<TopicAndPartition> topicAndPartitions,
                                                        Long before, int maxNumOffsets) {
    final Map<TopicAndPartition, Leader> tpToLeader = findLeaders(topicAndPartitions);
    List<TopicAndPartition> tps = Lists.newArrayList(tpToLeader.keySet());
    Function<TopicAndPartition, Leader> func = new Function<TopicAndPartition, Leader>() {
      @Nullable
      @Override
      public Leader apply(@Nullable TopicAndPartition tp) {
        return tpToLeader.get(tp);
      }
    };
    Multimap<Leader, TopicAndPartition> lToTp = Multimaps.index(tps, func);
    Map<TopicAndPartition, LeaderOffset> result = new HashMap<>();
    Set<Leader> leaders = lToTp.keySet();
    for (Leader leader : leaders) {
      SimpleConsumer consumer = null;
      try {
        consumer = connect(leader.getHost(), leader.getPort());
        List<TopicAndPartition> partitionsToGetOffsets =
                Lists.newArrayList(lToTp.get(new Leader(consumer.host(), consumer.port())));
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo;
        for (TopicAndPartition tp : partitionsToGetOffsets) {
          requestInfo = new HashMap<>();
          requestInfo.put(tp, new PartitionOffsetRequestInfo(before, maxNumOffsets));
          kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                  requestInfo
                  , OffsetRequest.CurrentVersion()
                  , OffsetRequest.DefaultClientId());
          OffsetResponse response = consumer.getOffsetsBefore(request);
          if (response.hasError()) {
            LOG.error("Error fetching data Offset Data the Broker. Reason: "
                    + response.errorCode(tp.topic(), tp.partition()));
          } else {
            long offset = response.offsets(tp.topic(), tp.partition())[0];
            result.put(tp, new LeaderOffset(consumer.host(), consumer.port(), offset));
          }
        }

      } catch (Exception e) {
        LOG.error("Error communicating with Broker [" + leader.getHost() + "] "
                + "to find Leader for [" + leader + "] Reason: ", e);
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }

    Set<TopicAndPartition> missing = Sets.difference(topicAndPartitions, result.keySet());
    //checkState(result.keySet().size() != topicAndPartitions.size(),
    if (result.keySet().size() != topicAndPartitions.size()) {
      LOG.error("Couldn't find leader offsets for " + missing);
      result = new HashMap<>();
    }

    return result;
  }

  Map<TopicAndPartition, LeaderOffset> getLatestLeaderOffsets(Set<TopicAndPartition> tps) {
    return leaderOffsets(tps, OffsetRequest.LatestTime());
  }


  Map<TopicAndPartition, LeaderOffset> getEarliestLeaderOffsets(Set<TopicAndPartition> tps) {
    return leaderOffsets(tps, OffsetRequest.EarliestTime());
  }

  Map<TopicAndPartition, LeaderOffset> leaderOffsets(Set<TopicAndPartition> tps, long before) {
    return getLeaderOffsets(tps, before, 1);
  }

}
