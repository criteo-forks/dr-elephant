/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.analysis;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


/**
 * The HDFS Information
 */
public final class HDFSContext {
  private static final Logger logger = Logger.getLogger(HDFSContext.class);

  public static long HDFS_BLOCK_SIZE = 64 * 1024 * 1024;
  public static final long DISK_READ_SPEED = 100 * 1024 * 1024;

  private HDFSContext() {
    // Empty on purpose
  }

  /**
   * Captures the HDFS Block Size
   * This is the default block size. Even if applications may override it, it is not the most common case,
   * and it will just raise a false negative in file limits.
   * Same about the blocksizes of different file systems. We decide to choose the lowest one.
   */
  public static void load() {
    try {
      HDFS_BLOCK_SIZE = minOfChildSystemsBlockSize(FileSystem.get(new Configuration()));
    } catch (IOException e) {
      logger.error("Error getting FS Block Size!", e);
    }

    logger.info("HDFS BLock size: " + HDFS_BLOCK_SIZE);
  }

  private static long minOfChildSystemsBlockSize(FileSystem fs) {
    FileSystem[] childFs = fs.getChildFileSystems();
    if (childFs != null) {
      Long[] sizes = new Long[childFs.length];
      for (int i = 0; i < childFs.length; i++) {
        sizes[i] = minOfChildSystemsBlockSize(childFs[i]);
      }
      return Collections.min(Arrays.asList(sizes));
    } else {
      return fs.getDefaultBlockSize(new Path("/"));
    }
  }
}
