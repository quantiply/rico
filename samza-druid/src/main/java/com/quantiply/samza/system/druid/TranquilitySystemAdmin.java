/*
 * Copyright 2016 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quantiply.samza.system.druid;

import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Map;
import java.util.Set;

public class TranquilitySystemAdmin implements SystemAdmin {
  private static final SystemAdmin singleton = new TranquilitySystemAdmin();

  private TranquilitySystemAdmin() {
    // Ensure this can not be constructed.
  }

  public static SystemAdmin getInstance() {
    return singleton;
  }

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(
      Map<SystemStreamPartition, String> map) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> set) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createChangelogStream(String stream, int foo) {
    throw new UnsupportedOperationException();
  }
}
