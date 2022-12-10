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

package org.apache.druid.segment.incremental;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.UnparseableColumnsParseException;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.utils.CircularBuffer;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * A handler for {@link ParseException}s thrown during ingestion. Based on the given configuration, this handler can
 *
 * - log ParseExceptions.
 * - keep most recent N ParseExceptions in memory.
 * - throw a RuntimeException when it sees more ParseExceptions than {@link #maxAllowedParseExceptions}.
 *
 * No matter what the handler does, the relevant metric should be updated first.
 */
public class ParseExceptionHandler
{
  private static final Logger LOG = new Logger(ParseExceptionHandler.class);

  private final RowIngestionMeters rowIngestionMeters;
  private final boolean logParseExceptions;
  private final int maxAllowedParseExceptions;
  @Nullable
  private final CircularBuffer<ParseExceptionReport> savedParseExceptionReports;
  @Nullable
  private final ServiceEmitter emitter;

  public ParseExceptionHandler(
      RowIngestionMeters rowIngestionMeters,
      boolean logParseExceptions,
      int maxAllowedParseExceptions,
      int maxSavedParseExceptions,
      ServiceEmitter emitter
  )
  {
    this.rowIngestionMeters = Preconditions.checkNotNull(rowIngestionMeters, "rowIngestionMeters");
    this.logParseExceptions = logParseExceptions;
    this.maxAllowedParseExceptions = maxAllowedParseExceptions;
    if (maxSavedParseExceptions > 0) {
      this.savedParseExceptionReports = new CircularBuffer<>(maxSavedParseExceptions);
    } else {
      this.savedParseExceptionReports = null;
    }
    this.emitter = emitter;
  }

  public void handle(@Nullable ParseException e, @Nullable Map<String, Object> taskMetadata)
  {
    if (e == null) {
      return;
    }

    if (e.isFromPartiallyValidRow()) {
      rowIngestionMeters.incrementProcessedWithError();
    } else {
      rowIngestionMeters.incrementUnparseable();
      if (emitter != null) {
        DateTime timeOfException = new DateTime(e.getTimeOfExceptionMillis());

        ImmutableMap<String, Object> extraData = new ImmutableMap.Builder().putAll(taskMetadata)
                                                                           .put("input", e.getInput())
                                                                           .put("detailMessage", e.getMessage())
                                                                           .build();
        Event event = new AlertEvent(
            timeOfException,
            "peon",
            "localhost",
            AlertEvent.Severity.DEFAULT,
            "unparseable Row at ingestion",
            extraData
        );
        emitter.emit(event);
      }

      logParseExceptionHelper(e);

      if (savedParseExceptionReports != null) {
        ParseExceptionReport parseExceptionReport = new ParseExceptionReport(
            e.getInput(),
            e.isFromPartiallyValidRow() ? "processedWithError" : "unparseable",
            e.isFromPartiallyValidRow()
            ? ((UnparseableColumnsParseException) e).getColumnExceptionMessages()
            : ImmutableList.of(e.getMessage()),
            e.getTimeOfExceptionMillis()
        );
        savedParseExceptionReports.add(parseExceptionReport);
      }


      if (rowIngestionMeters.getUnparseable() + rowIngestionMeters.getProcessedWithError()
          > maxAllowedParseExceptions) {
        throw new RE("Max parse exceptions[%s] exceeded", maxAllowedParseExceptions);
      }
    }
  }

  public void handle(@Nullable ParseException e)
  {
    handle(e, null);
  }

  @Nullable
  public CircularBuffer<ParseExceptionReport> getSavedParseExceptionReports()
  {
    return savedParseExceptionReports;
  }

  @VisibleForTesting
  public void logParseExceptionHelper(Exception e)
  {
    if (logParseExceptions) {
      LOG.error(e, "Encountered parse exception");
    }
  }
}
