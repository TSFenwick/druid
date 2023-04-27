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

package org.apache.druid.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class S3DataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(S3DataSegmentKiller.class);
  private static final String BUCKET = "bucket";
  private static final String KEY = "key";

  /**
   * Any implementation of DataSegmentKiller is initialized when an ingestion job starts if the extension is loaded,
   * even when the implementation of DataSegmentKiller is not used. As a result, if we have a s3 client instead
   * of a supplier of it, it can cause unnecessary config validation for s3 even when it's not used at all.
   * To perform the config validation only when it is actually used, we use a supplier.
   *
   * See OmniDataSegmentKiller for how DataSegmentKillers are initialized.
   */
  private final Supplier<ServerSideEncryptingAmazonS3> s3ClientSupplier;
  private final S3DataSegmentPusherConfig segmentPusherConfig;
  private final S3InputDataConfig inputDataConfig;

  @Inject
  public S3DataSegmentKiller(
      Supplier<ServerSideEncryptingAmazonS3> s3Client,
      S3DataSegmentPusherConfig segmentPusherConfig,
      S3InputDataConfig inputDataConfig
  )
  {
    this.s3ClientSupplier = s3Client;
    this.segmentPusherConfig = segmentPusherConfig;
    this.inputDataConfig = inputDataConfig;
  }

  @Override
  public void kill(List<DataSegment> segments) throws SegmentLoadingException
  {
    int size = segments.size();
    if (size == 0) {
      return;
    }
    if (segments.size() == 1) {
      kill(segments.get(0));
      return;

    }

    // we can assume that all segments are in the same bucket.
    String s3Bucket = MapUtils.getString(segments.get(0).getLoadSpec(), BUCKET);
    final ServerSideEncryptingAmazonS3 s3Client = this.s3ClientSupplier.get();

    List<DeleteObjectsRequest.KeyVersion> keysToDelete = segments.stream()
            .map(segment -> MapUtils.getString(segment.getLoadSpec(), KEY))
            .flatMap(path -> Stream.of(new DeleteObjectsRequest.KeyVersion(path),
                                     new DeleteObjectsRequest.KeyVersion(DataSegmentKiller.descriptorPath(path))))
            .collect(Collectors.toList());

    List<List<DeleteObjectsRequest.KeyVersion>> keysChunks = Lists.partition(keysToDelete, 1000);
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(s3Bucket);
    deleteObjectsRequest.setQuiet(true);
    // don't fail immediately delete as many as possible then fail in the end by throwing one exception that has all
    // exceptions in it
    List<Exception> exceptions = new ArrayList<>();
    for (List<DeleteObjectsRequest.KeyVersion> keysChunk : keysChunks) {
      try {
        deleteObjectsRequest.setKeys(keysChunk);
        log.info("Removing from bucket: %s the following index files: %s from s3!", s3Bucket, keysChunk);
        s3Client.deleteObjects(deleteObjectsRequest);
      } catch (AmazonServiceException e) {
        exceptions.add(e);
      }
    }
    if (exceptions.size() > 0) {
    List<String> segmentsNotDeleted =
        exceptions.stream().filter(exc -> exc instanceof MultiObjectDeleteException)
        .map(exc -> (MultiObjectDeleteException) exc)
        .map(MultiObjectDeleteException::getErrors)
        .flatMap(errors -> errors.stream().map(MultiObjectDeleteException.DeleteError::getKey))
        .collect(Collectors.toList());
      SegmentLoadingException segmentLoadingException =
          new SegmentLoadingException(exceptions.get(0), "For bucket: %s unable to delete some or all segments %s", s3Bucket, segmentsNotDeleted);
      for (int ii = 1; ii < exceptions.size(); ii++) {
        segmentLoadingException.addSuppressed(exceptions.get(ii));
      }
      throw segmentLoadingException;
    }
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, BUCKET);
      String s3Path = MapUtils.getString(loadSpec, KEY);
      String s3DescriptorPath = DataSegmentKiller.descriptorPath(s3Path);

      final ServerSideEncryptingAmazonS3 s3Client = this.s3ClientSupplier.get();
      if (s3Client.doesObjectExist(s3Bucket, s3Path)) {
        log.info("Removing index file[s3://%s/%s] from s3!", s3Bucket, s3Path);
        s3Client.deleteObject(s3Bucket, s3Path);
      }
      // descriptor.json is a file to store segment metadata in deep storage. This file is deprecated and not stored
      // anymore, but we still delete them if exists.
      if (s3Client.doesObjectExist(s3Bucket, s3DescriptorPath)) {
        log.info("Removing descriptor file[s3://%s/%s] from s3!", s3Bucket, s3DescriptorPath);
        s3Client.deleteObject(s3Bucket, s3DescriptorPath);
      }
    }
    catch (AmazonServiceException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getId(), e);
    }
  }

  @Override
  public void killAll() throws IOException
  {
    if (segmentPusherConfig.getBucket() == null || segmentPusherConfig.getBaseKey() == null) {
      throw new ISE(
          "Cannot delete all segment from S3 Deep Storage since druid.storage.bucket and druid.storage.baseKey are not both set.");
    }
    log.info("Deleting all segment files from s3 location [bucket: '%s' prefix: '%s']",
             segmentPusherConfig.getBucket(), segmentPusherConfig.getBaseKey()
    );
    try {
      S3Utils.deleteObjectsInPath(
          s3ClientSupplier.get(),
          inputDataConfig.getMaxListingLength(),
          segmentPusherConfig.getBucket(),
          segmentPusherConfig.getBaseKey(),
          Predicates.alwaysTrue()
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting segment files from s3. Error: %s", e.getMessage());
      throw new IOException(e);
    }
  }
}
