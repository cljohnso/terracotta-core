/*
 * Copyright (c) 2003-2006 Terracotta, Inc. All rights reserved.
 */
package com.tctest.performance.http.load;

import java.io.IOException;

public interface WorkItem {

  public void execute(StatsCollector c) throws IOException, NullPointerException, IllegalStateException;

  public boolean stop();

  public boolean expired(long currenttime);

  public void done();

}
