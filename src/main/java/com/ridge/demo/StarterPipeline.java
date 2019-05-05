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
package com.ridge.demo;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */

public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  private static final TupleTag<String> events = new TupleTag<String>(){};
  private static final TupleTag<String> nonEvents = new TupleTag<String>(){};

  static class FilterEvents extends DoFn<String, String> {

    @ProcessElement
    public void processElement(@Element String obj, MultiOutputReceiver out) {
      JSONParser jsonParser = new JSONParser();
      JSONObject jsonMessage = null;

      try {
        // Parse the context as a JSON object:
        jsonMessage = (JSONObject) jsonParser.parse(obj);
        if(((String)jsonMessage.get("type")).equals("event")) {
          out.get(events).output((String)jsonMessage.get("meta"));
        } else {
          out.get(nonEvents).output("not an event");
        }
      } catch (Exception e) {
        LOG.warn(String.format("Exception encountered parsing JSON (%s) ...", e));
      }
    } 
  }

  public static void main(String[] args) {
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    PCollectionTuple t = p.apply("read from PubSub",
      PubsubIO.readStrings().fromSubscription("projects/gfs-ecm/subscriptions/df-wibble-test-sub")
    )
    .apply("Filter events",
      ParDo.of(
        new FilterEvents()
      )
      .withOutputTags(events, TupleTagList.of(nonEvents))
    );

    t.get(events)
    .apply("Log out events",
      ParDo.of(
        new DoFn<String, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c)  {
            LOG.info(c.element());
          }
        }
      )
    );
    
    t.get(nonEvents)
    .apply("Log non events",
      ParDo.of(
        new DoFn<String, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c)  {
            LOG.info("Non-event found");
          }
        }
      )
    );

    p.run();
  }
}
