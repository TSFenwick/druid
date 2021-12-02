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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.NoneGranularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InformationSchemaTest
{
  InformationSchema informationSchema;

  @Before
  public void setUp()
  {
    informationSchema = new InformationSchema(new DruidSchemaCatalog(null, null), null, TestHelper.makeJsonMapper());
  }

  @Test
  public void testPeriodGranularity()
  {
    String output = informationSchema.getReadableGranularity(new PeriodGranularity(
        Period.hours(6),
        DateTime.parse("2021-11-30T16:35:11.793-08:00"),
        DateTimeZone.UTC
    ));
    Assert.assertEquals(
        "{\"type\":\"period\",\"period\":\"PT6H\",\"timeZone\":\"UTC\",\"origin\":\"2021-12-01T00:35:11.793Z\"}",
        output
    );
  }

  @Test
  public void testNoneGranularity()
  {
    String output = informationSchema.getReadableGranularity(new NoneGranularity());
    Assert.assertEquals("{\"type\":\"none\"}", output);
  }

  @Test
  public void testDurationGranularity()
  {
    String output = informationSchema.getReadableGranularity(new DurationGranularity(
        100000000000023L,
        10000000000000023L
    ));
    Assert.assertEquals(
        "{\"type\":\"duration\",\"duration\":100000000000023,\"origin\":\"5138-11-16T09:46:37.746Z\"}",
        output
    );
  }

  @Test
  public void testAllGranularity()
  {
    String output = informationSchema.getReadableGranularity(new AllGranularity());
    Assert.assertEquals("{\"type\":\"all\"}", output);
  }

  @Test
  public void testStandardGranularities()
  {
    String output = informationSchema.getReadableGranularity(Granularities.YEAR);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"P1Y\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.QUARTER);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"P3M\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.MONTH);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"P1M\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.WEEK);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"P1W\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.DAY);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"P1D\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.SIX_HOUR);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"PT6H\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.HOUR);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"PT1H\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.THIRTY_MINUTE);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"PT30M\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.FIFTEEN_MINUTE);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"PT15M\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.TEN_MINUTE);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"PT10M\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.FIVE_MINUTE);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"PT5M\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.MINUTE);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"PT1M\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.SECOND);
    Assert.assertEquals("{\"type\":\"period\",\"period\":\"PT1S\",\"timeZone\":\"UTC\",\"origin\":null}", output);
    output = informationSchema.getReadableGranularity(Granularities.NONE);
    Assert.assertEquals("{\"type\":\"none\"}", output);
    output = informationSchema.getReadableGranularity(Granularities.ALL);
    Assert.assertEquals("{\"type\":\"all\"}", output);
  }
}
