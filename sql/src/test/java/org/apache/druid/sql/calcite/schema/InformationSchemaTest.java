package org.apache.druid.sql.calcite.schema;

import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.NoneGranularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
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
    informationSchema = new InformationSchema(new DruidSchemaCatalog(null, null), null);
  }

  @Test
  public void testPeriodGranularity()
  {

    String output = informationSchema.getReadableGranularity(new PeriodGranularity(
        Period.hours(6),
        DateTime.parse("2021-11-30T16:35:11.793-08:00"),
        DateTimeZone.UTC
    ));
    Assert.assertEquals("{\"type\":\"period\",\"period\":{\"days\":0,\"fieldTypes\":[{\"name\":\"years\"},{\"name\":\"months\"},{\"name\":\"weeks\"},{\"name\":\"days\"},{\"name\":\"hours\"},{\"name\":\"minutes\"},{\"name\":\"seconds\"},{\"name\":\"millis\"}],\"hours\":6,\"millis\":0,\"minutes\":0,\"months\":0,\"periodType\":{\"name\":\"Standard\"},\"seconds\":0,\"values\":[0,0,0,0,6,0,0,0],\"weeks\":0,\"years\":0},\"timeZone\":{\"fixed\":true,\"id\":\"UTC\"},\"origin\":{\"afterNow\":false,\"beforeNow\":true,\"centuryOfEra\":20,\"chronology\":{\"zone\":{\"fixed\":true,\"id\":\"UTC\"}},\"dayOfMonth\":1,\"dayOfWeek\":3,\"dayOfYear\":335,\"equalNow\":false,\"era\":1,\"hourOfDay\":0,\"millis\":1638318911793,\"millisOfDay\":2111793,\"millisOfSecond\":793,\"minuteOfDay\":35,\"minuteOfHour\":35,\"monthOfYear\":12,\"secondOfDay\":2111,\"secondOfMinute\":11,\"weekOfWeekyear\":48,\"weekyear\":2021,\"year\":2021,\"yearOfCentury\":21,\"yearOfEra\":2021,\"zone\":{\"fixed\":true,\"id\":\"UTC\"}}}", output);
  }

  @Test
  public void testNoneGranularity()
  {
    String output = informationSchema.getReadableGranularity(new NoneGranularity());
    Assert.assertEquals("NONE", output);
  }

  @Test
  public void testDurationGranularity()
  {
    String output = informationSchema.getReadableGranularity(new DurationGranularity(100000000000023L, 10000000000000023L));
    Assert.assertEquals("{\"duration\":100000000000023,\"origin\":{\"afterNow\":true,\"beforeNow\":false,\"centuryOfEra\":51,\"chronology\":{\"zone\":{\"fixed\":true,\"id\":\"UTC\"}},\"dayOfMonth\":16,\"dayOfWeek\":3,\"dayOfYear\":320,\"equalNow\":false,\"era\":1,\"hourOfDay\":9,\"millis\":99999999997746,\"millisOfDay\":35197746,\"millisOfSecond\":746,\"minuteOfDay\":586,\"minuteOfHour\":46,\"monthOfYear\":11,\"secondOfDay\":35197,\"secondOfMinute\":37,\"weekOfWeekyear\":46,\"weekyear\":5138,\"year\":5138,\"yearOfCentury\":38,\"yearOfEra\":5138,\"zone\":{\"fixed\":true,\"id\":\"UTC\"}},\"cacheKey\":\"AABa8xB6QBcAAFrzEHo3Mg==\",\"durationMillis\":100000000000023}", output);
  }

  @Test
  public void testAllGranularity()
  {
    String output = informationSchema.getReadableGranularity(new AllGranularity());
    Assert.assertEquals("ALL", output);
  }

  @Test
  public void testStandardGranularities()
  {
    String output = informationSchema.getReadableGranularity(Granularities.YEAR);
    Assert.assertEquals("YEAR", output);
    output = informationSchema.getReadableGranularity(Granularities.QUARTER);
    Assert.assertEquals("QUARTER", output);
    output = informationSchema.getReadableGranularity(Granularities.MONTH);
    Assert.assertEquals("MONTH", output);
    output = informationSchema.getReadableGranularity(Granularities.WEEK);
    Assert.assertEquals("WEEK", output);
    output = informationSchema.getReadableGranularity(Granularities.DAY);
    Assert.assertEquals("DAY", output);
    output = informationSchema.getReadableGranularity(Granularities.SIX_HOUR);
    Assert.assertEquals("SIX_HOUR", output);
    output = informationSchema.getReadableGranularity(Granularities.HOUR);
    Assert.assertEquals("HOUR", output);
    output = informationSchema.getReadableGranularity(Granularities.THIRTY_MINUTE);
    Assert.assertEquals("THIRTY_MINUTE", output);
    output = informationSchema.getReadableGranularity(Granularities.FIFTEEN_MINUTE);
    Assert.assertEquals("FIFTEEN_MINUTE", output);
    output = informationSchema.getReadableGranularity(Granularities.TEN_MINUTE);
    Assert.assertEquals("TEN_MINUTE", output);
    output = informationSchema.getReadableGranularity(Granularities.FIVE_MINUTE);
    Assert.assertEquals("FIVE_MINUTE", output);
    output = informationSchema.getReadableGranularity(Granularities.MINUTE);
    Assert.assertEquals("MINUTE", output);
    output = informationSchema.getReadableGranularity(Granularities.SECOND);
    Assert.assertEquals("SECOND", output);
    output = informationSchema.getReadableGranularity(Granularities.ALL);
    Assert.assertEquals("ALL", output);
    output = informationSchema.getReadableGranularity(Granularities.NONE);
    Assert.assertEquals("NONE", output);
  }
}
