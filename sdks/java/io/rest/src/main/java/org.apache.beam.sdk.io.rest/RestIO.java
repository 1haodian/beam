/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.rest;

//import static com.google.common.base.Preconditions.*;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Experimental
public class RestIO {

  private static final Logger LOG = LoggerFactory.getLogger(RestIO.class);

  public static Read read() {
    return new AutoValue_RestIO_Read.Builder().build();
  }

  public static Write write() {
    return new AutoValue_RestIO_Write();
  }

  private RestIO() {

  }

  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    @Nullable abstract String location();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setLocation(String location);
      abstract Read build();
    }

    public Read withLocation(String location) {
     /* checkArgument(location != null, "RestIO.read().withLocation(location) called with null "
          + "location");*/
      return builder().setLocation(location).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input.apply(Create.of(location()))
          .apply(ParDo.of(new ReadFn(this)));
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      /*checkState(location() != null, "RestIO.read() requires a location to be set via "
          + "withLocation(location)");*/
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.addIfNotNull(DisplayData.item("location", location()));
    }

  }

  static class ReadFn extends DoFn<String, String> {

    private final Read spec;
    private DefaultHttpClient client;

    private ReadFn(Read spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      client = new DefaultHttpClient();
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      String location = processContext.element();
      HttpGet httpGet = new HttpGet(location);
      BasicHeader basicHeader = new BasicHeader("Accept", "application/json");
      httpGet.addHeader(basicHeader);
      HttpResponse httpResponse = client.execute(httpGet);
      HttpEntity httpEntity = httpResponse.getEntity();
      String response = EntityUtils.toString(httpEntity);
      processContext.output(response);
    }

  }

  @AutoValue
  public abstract static class Write extends PTransform<PCollection<KV<String, String>>, PDone> {

    @Override
    public PDone expand(PCollection<KV<String, String>> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

  }

  private static class WriteFn extends DoFn<KV<String, String>, Void> {

    private final Write spec;

    private DefaultHttpClient client;

    public WriteFn(Write spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      client = new DefaultHttpClient();
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      KV<String, String> kv = processContext.element();
      HttpPost post = new HttpPost(kv.getKey());
      StringEntity entity = new StringEntity(kv.getValue());
      post.setEntity(entity);
      client.execute(post);
    }
  }

}