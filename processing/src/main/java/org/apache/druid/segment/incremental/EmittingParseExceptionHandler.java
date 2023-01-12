package org.apache.druid.segment.incremental;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.service.AlertBuilder;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public class EmittingParseExceptionHandler extends ParseExceptionHandler
{
  private final ServiceEmitter emitter;
  private final Map<String, Object> extraData;

  public EmittingParseExceptionHandler(
      RowIngestionMeters rowIngestionMeters,
      boolean logParseExceptions,
      int maxAllowedParseExceptions,
      int maxSavedParseExceptions,
      ServiceEmitter emitter,
      String taskId,
      String groupId,
      String dataSource
  )
  {
    super(rowIngestionMeters, logParseExceptions, maxAllowedParseExceptions, maxSavedParseExceptions);
    this.emitter = emitter;
    this.extraData = ImmutableMap.of("taskId", taskId, "groupId", groupId, "dataSource", dataSource);
  }

  public EmittingParseExceptionHandler(
      RowIngestionMeters rowIngestionMeters,
      boolean logParseExceptions,
      int maxAllowedParseExceptions,
      int maxSavedParseExceptions,
      ServiceEmitter emitter
  )
  {
    super(rowIngestionMeters, logParseExceptions, maxAllowedParseExceptions, maxSavedParseExceptions);
    this.emitter = emitter;
    this.extraData = ImmutableMap.of();
  }
  @Override
  public void handle(@Nullable ParseException e)
  {
    super.handle(e);
  }

  public void handle(@Nullable ParseException e, @Nonnull Map<String, Object> taskMetadata)
  { if (e== null) {
      return;
    }
    DateTime timeOfException = DateTimes.utc(e.getTimeOfExceptionMillis());

    Map<String, Object> moreData = new ImmutableMap.Builder().putAll(taskMetadata)
                                                                       .put("input", e.getInput())
                                                                       .put("detailMessage", e.getMessage())
                                                                       .putAll(this.extraData)
                                                                       .build();
    AlertBuilder alertBuilder = AlertBuilder.createEmittable(emitter, "", moreData);
    emitter.emit(alertBuilder);
    this.handle(e);
  }
}
