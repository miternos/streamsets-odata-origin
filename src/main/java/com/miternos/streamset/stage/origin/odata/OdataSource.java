/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.miternos.streamset.stage.origin.odata;

import com.miternos.streamset.stage.lib.Errors;
import com.miternos.streamset.stage.util.OdataClient;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * This source is an example and does not actually read from anywhere.
 * It does however, generate generate a simple record with one field.
 */
public abstract class OdataSource extends BaseSource {

    public static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(
            OdataSource.class);

    private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private static LinkedBlockingQueue<Map<String,String>> queue = new LinkedBlockingQueue<>();

    private OdataClient client;

    @Override protected List<ConfigIssue> init() {

        Date startDate = new Date(0L); // Default start date is beginning of time

        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        if (StringUtils.isEmpty(getDeviceNetworkId())) {
            issues.add(getContext().createConfigIssue(Groups.ODATA.name(), "deviceNetworkId", Errors.ERROR_00,
                                                      "DeviceNetworkId missing"));
        }
        if (StringUtils.isEmpty(getAuthToken())) {
            issues.add(getContext().createConfigIssue(Groups.ODATA.name(), "authToken", Errors.ERROR_00,
                                                      "AuthToken missing"));
        }
        if (StringUtils.isEmpty(getOdataUrl())) {
            issues.add(getContext().createConfigIssue(Groups.ODATA.name(), "odataUrl", Errors.ERROR_00,
                                                      "Odata Url missing"));
        }
        if (StringUtils.isEmpty(getResourceRefreshPeriod())) {
            issues.add(getContext().createConfigIssue(Groups.ODATA.name(), "resourceRefreshPeriod", Errors.ERROR_00,
                                                      "Refresh Period missing "));
        }

        if (StringUtils.isEmpty(getResourceType())) {
            issues.add(getContext().createConfigIssue(Groups.ODATA.name(), "resourceType", Errors.ERROR_00,
                                                      "Resource Type is missing "));
        }

        if ( !StringUtils.isEmpty(getStartDateTime())){
            try {
                startDate = df.parse(getStartDateTime());
            } catch (ParseException e) {
                issues.add(getContext().createConfigIssue(Groups.ODATA.name(), "startDateTime", Errors.ERROR_00,
                                                          "Format is not correct should be yyyy-MM-dd'T'HH:mm:ssZ "));
            }
        }

        if ( issues.size() == 0 ){

            try {
                long refreshPeriod = Long.valueOf(getResourceRefreshPeriod());

                client = new OdataClient(getDeviceNetworkId(), getAuthToken(), getOdataUrl(), refreshPeriod, startDate,getResourceType());

                logger.info("Odata client thread created");

                if (executorService.isShutdown())
                    executorService = Executors.newSingleThreadScheduledExecutor();

                executorService
                        .scheduleAtFixedRate(client, 0, Long.valueOf(getResourceRefreshPeriod()), TimeUnit.SECONDS);
                logger.info("Odata client thread submitted");

            } catch (NumberFormatException e){
                issues.add(getContext().createConfigIssue(Groups.ODATA.name(), "resourceRefreshPeriod", Errors.ERROR_00,
                                                          "Refresh Period not valid "));
            }

        }
        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        // Clean up any open resources.
        if ( client != null)
            client.disconnect();

        try {
            logger.error("attempt to shutdown odata executor");
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("odata tasks interrupted");
        } finally {
            if (!executorService.isTerminated()) {
                logger.error("cancel non-finished odata tasks");
            }
            executorService.shutdownNow();
            logger.error("shutdown odata finished");
        }

        super.destroy();
    }

    /** {@inheritDoc} */
    @Override public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws
                                                                                                      StageException {

        // Offsets can vary depending on the data source. Here we use an integer as an example only.
        long nextSourceOffset = 0;
        if (lastSourceOffset != null) {
            nextSourceOffset = Long.parseLong(lastSourceOffset);
        }

        int numRecords = 0;

        if (client != null) {
            try {
                while (numRecords < maxBatchSize) {
                    Map item = client.take();                                 // Blocking
                    logger.debug("Add to batchMaker NumRecords:"+numRecords+" maxBatchSize:"+maxBatchSize+" Item: "+item);

                    Record r = getContext().createRecord(String.valueOf(numRecords));

                    Map<String, Field> fieldMap = new HashMap<>();

                    for ( Object e: item.keySet() ){
                        String key = (String)e;
                        Object val = item.get(key);
                        fieldMap.put(key,Field.create(String.valueOf(val)));
                    }

                    r.set(Field.create(fieldMap));
                    batchMaker.addRecord(r);
                    ++nextSourceOffset;
                    ++numRecords;
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }

        }

        logger.debug("Return from produce wit offset: "+nextSourceOffset);
        return String.valueOf(nextSourceOffset);
    }

    public abstract String getDeviceNetworkId();

    public abstract String getAuthToken();

    public abstract String getOdataUrl();

    public abstract String getResourceRefreshPeriod();

    public abstract String getResourceType();

    public abstract String getStartDateTime();
}
