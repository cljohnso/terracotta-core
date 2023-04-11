/*
 *
 *  The contents of this file are subject to the Terracotta Public License Version
 *  2.0 (the "License"); You may not use this file except in compliance with the
 *  License. You may obtain a copy of the License at
 *
 *  http://terracotta.org/legal/terracotta-public-license.
 *
 *  Software distributed under the License is distributed on an "AS IS" basis,
 *  WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
 *  the specific language governing rights and limitations under the License.
 *
 *  The Covered Software is Terracotta Core.
 *
 *  The Initial Developer of the Covered Software is
 *  Terracotta, Inc., a Software AG company
 *
 */
package com.tc.objectserver.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tc.management.AbstractTerracottaMBean;
import javax.management.NotCompliantMBeanException;

import static java.lang.String.join;
import java.util.Set;
import java.util.function.Supplier;
import org.terracotta.server.ServerEnv;

public class TopologyManager {

  private final Supplier<Set<String>> config;
  private volatile TopologyMbean topologyMbean;
  private final Supplier<Integer> voters;

  public TopologyManager(Supplier<Set<String>> config, Supplier<Integer> voters) {
    this.config = config;
    this.voters = voters;
    initializeMbean();
  }

  public int getExternalVoters() {
    return voters.get();
  }

  public boolean isAvailability() {
    return voters.get() < 0;
  }

  public Topology getTopology() {
    return new Topology(config.get());
  }

  private void initializeMbean() {
    if (topologyMbean != null) return;
    try {
      this.topologyMbean = new TopologyMbeanImpl(this);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException(e);
    }
  }

  public interface TopologyMbean {
    String getTopology();
  }

  private static class TopologyMbeanImpl extends AbstractTerracottaMBean implements TopologyMbean {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyMbeanImpl.class);
    private final TopologyManager topologyManager;

    TopologyMbeanImpl(TopologyManager topologyManager) throws NotCompliantMBeanException {
      super(TopologyMbean.class, false);
      this.topologyManager = topologyManager;

      try {
        ServerEnv.getServer().getManagement().registerMBean("TopologyMBean", this);
      } catch (Exception e) {
        LOGGER.warn("Problem registering TopologyMBean", e);
      }
    }

    @Override
    public String getTopology() {
      return join(",", this.topologyManager.getTopology().getServers());
    }

    @Override
    public void reset() {
      // no-op
    }
  }
}
