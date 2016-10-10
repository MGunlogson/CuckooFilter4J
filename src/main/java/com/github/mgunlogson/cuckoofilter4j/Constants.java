/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Copied from Apache Harmony and Lucene (6.2.0) projects with modifications
 */
package com.github.mgunlogson.cuckoofilter4j;

/**
 * Some useful constants.
 **/

final class Constants {
  private Constants() {}  // can't construct

  static final String OS_ARCH = System.getProperty("os.arch");

 
  /** True iff running on a 64bit JVM */
  static final boolean JRE_IS_64BIT;
  
  static {
    boolean is64Bit = false;
    final String x = System.getProperty("sun.arch.data.model");
    if (x != null) {
      is64Bit = x.contains("64");
    } else {
      if (OS_ARCH != null && OS_ARCH.contains("64")) {
        is64Bit = true;
      } else {
        is64Bit = false;
      }
    }
    JRE_IS_64BIT = is64Bit;
  }

}