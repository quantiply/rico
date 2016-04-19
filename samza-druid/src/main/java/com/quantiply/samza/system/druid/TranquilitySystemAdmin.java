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
