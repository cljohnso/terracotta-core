/*
 *  Copyright Terracotta, Inc.
 *  Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.tc.objectserver.handler;

import com.tc.async.api.MultiThreadedEventContext;


public class ReplicationReceivingAction implements MultiThreadedEventContext, Runnable {

  private final Runnable response;
  private final Integer destination;

  public ReplicationReceivingAction(Integer skew, Runnable response) {
    this.destination = skew;
    this.response = response;
  }

  @Override
  public void run() {
    response.run();
  }
  
  @Override
  public Object getSchedulingKey() {
    return destination;
  }

  @Override
  public boolean flush() {
    return false;
  }

}
