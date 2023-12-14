package org.apache.druid.msq.indexing.error;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceLogEvent;

import java.io.IOException;
import java.util.Set;

public class MSQFilteredEmitterWarningPublisher implements MSQWarningPublisher
{

  private final String workerId;
  private final String controllerTaskId;
  private final String taskId;
  private final String host;
  private final ServiceEmitter emitter;
  private final Set<String> acceptableErrorCode;
  public MSQFilteredEmitterWarningPublisher(
      String workerId,
      String controllerTaskId,
      String taskId,
      String host,
      ServiceEmitter emitter,
      Set<String> acceptableErrorCode
  )
  {
    this.workerId = workerId;
    this.controllerTaskId = controllerTaskId;
    this.taskId = taskId;
    this.host = host;
    this.emitter = emitter;
    this.acceptableErrorCode = acceptableErrorCode;
  }

  @Override
  public void publishException(int stageNumber, Throwable e)
  {
    String errorCode = MSQErrorReport.getFaultFromException(e).getErrorCode();
    if (acceptableErrorCode.contains(errorCode)) {
      MSQErrorReport errorReport = MSQErrorReport.fromException(workerId, host, stageNumber, e);
      String errorMessage = errorReport.getFault().getErrorMessage();
      ServiceLogEvent.Builder serviceLogEvent = new ServiceLogEvent.Builder();
      serviceLogEvent.setData(ImmutableMap.<String, Object>builder()
                                          .put("message", errorMessage)
                                          .put("host", errorReport.getHost())
                                          .put("workerId", workerId)
                                          .put("stageNumber", stageNumber)
                                          .put("errorCode", errorCode)
                                          .build());
      emitter.emit(serviceLogEvent.build(DateTimes.nowUtc()));
    }
  }

  @Override
  public void close() throws IOException
  {

  }
}
