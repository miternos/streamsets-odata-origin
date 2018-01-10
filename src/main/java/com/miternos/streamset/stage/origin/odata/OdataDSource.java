/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.miternos.streamset.stage.origin.odata;

import com.streamsets.pipeline.api.*;

@StageDef(
    version = 1,
    label = "Odata",
    description = "Odata Origin to get records from",
    icon = "odata.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class OdataDSource extends OdataSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      label = "Network Id",
      description = "Network Id",
      displayPosition = 10,
      group = "ODATA"
  ) public String deviceNetworkId;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "SensorMeasures",
          label = "SensorMeasures",
          description = "Resource to collect",
          displayPosition = 10,
          group = "ODATA"
  ) public String resourceType;

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.STRING,
          defaultValue = "2018-01-01T00:00:00+03:00",
          label = "Start DateTime",
          description = "Start time in yyyy-MM-dd'T'HH:mm:ssX format. Leave empty to start from beginning",
          displayPosition = 10,
          group = "ODATA"
  ) public String startDateTime;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "**********",
          label = "Authentication Token",
          displayPosition = 10,
          group = "ODATA"
  ) public String authToken;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "http://some.odata.url/",
          label = "Odata Url",
          displayPosition = 10,
          group = "ODATA"
  ) public String odataUrl;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "60",
          label = "Refresh period in secs to get data from odata",
          displayPosition = 10,
          group = "ODATA"
  ) public String resourceRefreshPeriod;




  @Override
  public String getDeviceNetworkId() {
    return deviceNetworkId;
  }

  @Override
  public String getAuthToken() {
    return authToken;
  }

  @Override
  public String getOdataUrl() {
    return odataUrl;
  }

  @Override
  public String getResourceRefreshPeriod() {
    return resourceRefreshPeriod;
  }

  @Override
  public String getResourceType() {
    return resourceType;
  }

  @Override
  public String getStartDateTime() {
    return startDateTime;
  }
}
