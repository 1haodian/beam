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

import com.google.common.base.Objects;

import java.io.Serializable;

import kafka.javaapi.message.ByteBufferMessageSet;

/**
 * KafkaMessage contains {@link ByteBufferMessageSet} as well as metadata for the message (topic name,
 * partition id).
 */
public class KafkaMessage implements Serializable {
  private ByteBufferMessageSet byteBufferMessageSet;
  private final String topic;
  private final int partition;


  public KafkaMessage(
          String topic,
          int partition,
          ByteBufferMessageSet byteBufferMessageSet) {

    this.topic = topic;
    this.partition = partition;
    this.byteBufferMessageSet = byteBufferMessageSet;
  }

  public ByteBufferMessageSet getByteBufferMessageSet() {
    return byteBufferMessageSet;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KafkaMessage that = (KafkaMessage) o;
    return partition == that.partition
            && Objects.equal(byteBufferMessageSet, that.byteBufferMessageSet)
            && Objects.equal(topic, that.topic);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(byteBufferMessageSet, topic, partition);
  }
}
