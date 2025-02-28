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

package org.apache.druid.emitter.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.SegmentMetadataEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.DefaultRequestLogEventBuilderFactory;
import org.apache.druid.server.log.RequestLogEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class KafkaEmitterTest
{
  @Parameterized.Parameter(0)
  public Set<KafkaEmitterConfig.EventType> eventsType;

  @Parameterized.Parameter(1)
  public String requestTopic;

  @Parameterized.Parameters(name = "{index}: eventTypes - {0}, requestTopic - {1}")
  public static Object[] data()
  {
    return new Object[][] {
        {new HashSet<>(Arrays.asList(KafkaEmitterConfig.EventType.METRICS, KafkaEmitterConfig.EventType.REQUESTS, KafkaEmitterConfig.EventType.ALERTS, KafkaEmitterConfig.EventType.SEGMENT_METADATA)), "requests"},
        {new HashSet<>(Arrays.asList(KafkaEmitterConfig.EventType.METRICS, KafkaEmitterConfig.EventType.ALERTS, KafkaEmitterConfig.EventType.SEGMENT_METADATA)), null}
    };
  }

  // there is 1 seconds wait in kafka emitter before it starts sending events to broker, set a timeout for 10 seconds
  @Test(timeout = 10_000)
  public void testKafkaEmitter() throws InterruptedException
  {
    final List<ServiceMetricEvent> serviceMetricEvents = ImmutableList.of(
        ServiceMetricEvent.builder().setMetric("m1", 1).build("service", "host")
    );

    final List<AlertEvent> alertEvents = ImmutableList.of(
        new AlertEvent("service", "host", "description")
    );

    final List<RequestLogEvent> requestLogEvents = ImmutableList.of(
        DefaultRequestLogEventBuilderFactory.instance().createRequestLogEventBuilder("requests",
            RequestLogLine.forSql("", null, DateTimes.nowUtc(), null, new QueryStats(ImmutableMap.of()))
        ).build("service", "host")
    );

    final List<SegmentMetadataEvent> segmentMetadataEvents = ImmutableList.of(
        new SegmentMetadataEvent(
            "dummy_datasource",
            DateTimes.of("2001-01-01T00:00:00.000Z"),
            DateTimes.of("2001-01-02T00:00:00.000Z"),
            DateTimes.of("2001-01-03T00:00:00.000Z"),
            "dummy_version",
            true
        )
    );

    int totalEvents = serviceMetricEvents.size() + alertEvents.size() + requestLogEvents.size() + segmentMetadataEvents.size();
    int totalEventsExcludingRequestLogEvents = totalEvents - requestLogEvents.size();

    final CountDownLatch countDownSentEvents = new CountDownLatch(
        requestTopic == null ? totalEventsExcludingRequestLogEvents : totalEvents);

    final KafkaProducer<String, String> producer = mock(KafkaProducer.class);
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JodaModule());
    final KafkaEmitter kafkaEmitter = new KafkaEmitter(
        new KafkaEmitterConfig("", eventsType, "metrics", "alerts", requestTopic, "metadata", "test-cluster", null, null, null),
        mapper
    )
    {
      @Override
      protected Producer<String, String> setKafkaProducer()
      {
        // override send interval to 1 second
        sendInterval = 1;
        return producer;
      }
    };

    when(producer.send(any(), any())).then((invocation) -> {
      countDownSentEvents.countDown();
      return null;
    });

    kafkaEmitter.start();

    for (Event event : serviceMetricEvents) {
      kafkaEmitter.emit(event);
    }
    for (Event event : alertEvents) {
      kafkaEmitter.emit(event);
    }
    for (Event event : requestLogEvents) {
      kafkaEmitter.emit(event);
    }
    for (Event event : segmentMetadataEvents) {
      kafkaEmitter.emit(event);
    }
    countDownSentEvents.await();

    kafkaEmitter.close();

    Assert.assertEquals(0, kafkaEmitter.getMetricLostCount());
    Assert.assertEquals(0, kafkaEmitter.getAlertLostCount());
    Assert.assertEquals(0, kafkaEmitter.getSegmentMetadataLostCount());
    Assert.assertEquals(requestTopic == null ? requestLogEvents.size() : 0, kafkaEmitter.getRequestLostCount());
    Assert.assertEquals(0, kafkaEmitter.getInvalidLostCount());
  }
}
