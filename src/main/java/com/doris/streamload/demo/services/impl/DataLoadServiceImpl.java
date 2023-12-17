// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.doris.streamload.demo.services.impl;

import com.doris.streamload.demo.beans.DataBean;
import com.doris.streamload.demo.beans.DataValueEnums;
import com.doris.streamload.demo.conf.DorisConfig;
import com.doris.streamload.demo.services.DataLoadService;
import com.doris.streamload.demo.services.impl.convertor.IConvertorImpl;
import org.apache.doris.streamload.core.IConvertor;
import org.apache.doris.streamload.core.StreamLoad;
import org.apache.doris.streamload.core.exception.StreamLoadException;
import org.apache.doris.streamload.core.params.DorisContentParams;
import org.apache.doris.streamload.core.params.StreamLoadParams;
import org.apache.doris.streamload.core.params.StreamLoadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
public class DataLoadServiceImpl implements DataLoadService {
    @Resource
    DorisConfig dorisConfig;
    private static final Logger log = LoggerFactory.getLogger(DataLoadServiceImpl.class);
    private int batchSize = 100000;
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private List<DataBean> dataList = new ArrayList<>();


    /**
     * 3. Implement the IConvertor interface method, complete data processing according to its
     * own business processing logic, and build an IConvertor implementation class.
     */
    private IConvertor iConvertor = new IConvertorImpl();

    /**
     * 4. Create the StreamLoad-Core class, execute the data loading logic in batches, and control
     * the batch data loading amount according to the DataSize parameter. The default is 10,000 items per batch.
     */
    private StreamLoad streamLoad = new StreamLoad(iConvertor);

    @Override
    public DataBean sourceData() {
        return new DataBean(
                new Random().nextLong() & Long.MAX_VALUE
                , DataValueEnums.USER_IDS[new Random().nextInt(9)]
                , DataValueEnums.EVENT_TYPES[new Random().nextInt(9)]
                , timeFormatter.format(LocalDateTime.now())
                , DataValueEnums.PAGE_NAMES[new Random().nextInt(9)]
                , DataValueEnums.ACTION_DETAILS[new Random().nextInt(9)]
                , DataValueEnums.DEVICE_TYPES[new Random().nextInt(9)]
                , DataValueEnums.OS_VERSIONS[new Random().nextInt(9)]
                , DataValueEnums.APP_VERSIONS[new Random().nextInt(9)]
                , DataValueEnums.GEO_LOCATIONS[new Random().nextInt(9)]
                , DataValueEnums.NETWORK_TYPES[new Random().nextInt(9)]
        );
    }

    @Override
    public String sinkDataWithJSON(int dataSize) {
        long startTime = System.currentTimeMillis();
        // 1. When creating the connection Doris, pass the parameter object 'DorisContentParams' and assign the value.
        DorisContentParams dorisContentParams = new DorisContentParams(dorisConfig.getHost(), dorisConfig.getHttpPort(),
                dorisConfig.getDatabase(), dorisConfig.getTable(), dorisConfig.getUsername(), dorisConfig.getPassword()
        );
        // 2. Create the parameter object 'StreamLoadParams' when importing using StreamLoad this time,
        // and set the corresponding parameters.
        StreamLoadResult streamLoadResult = new StreamLoadResult();
        for (int i = 0; i < dataSize; i++) {
            dataList.add(sourceData());
            if ((i != 0 && i % batchSize == 0) || (i + 1 == dataSize)) {
                StreamLoadParams build = new StreamLoadParams.Builder()
                        .setFormat("json")
                        .setFuzzyParse("true")
                        .setStrictMode("true")
                        .setStripOuterArray("true")
                        .build();
                try {
                    streamLoadResult = streamLoad.run(dataList, dorisContentParams, build);
                } catch (StreamLoadException e) {
                    throw new RuntimeException(e);
                }
                dataList.clear();
                log.info(streamLoadResult.toString());
            }
        }
        long endTime = System.currentTimeMillis();
        if (!"Success".equals(streamLoadResult.getStatus())) {
            throw new RuntimeException(streamLoadResult.toString());
        }
        return String.format("All have been successfully imported, a total of %s items, " +
                "and the cumulative time taken is %s Ms", dataSize, endTime - startTime);
    }

    @Override
    public String sinkDataWithCSV(int dataSize) {
        long startTime = System.currentTimeMillis();
        // 1. When creating the connection Doris, pass the parameter object 'DorisContentParams' and assign the value.
        DorisContentParams dorisContentParams = new DorisContentParams(dorisConfig.getHost(), dorisConfig.getHttpPort(),
                dorisConfig.getDatabase(), dorisConfig.getTable(), dorisConfig.getUsername(), dorisConfig.getPassword()
        );
        // 2. Create the parameter object 'StreamLoadParams' when importing using StreamLoad this time,
        // and set the corresponding parameters.
        StreamLoadResult streamLoadResult = new StreamLoadResult();
        for (int i = 0; i < dataSize; i++) {
            dataList.add(sourceData());
            if ((i != 0 && i % batchSize == 0) || (i + 1 == dataSize)) {
                StreamLoadParams build = new StreamLoadParams.Builder()
                        .setFormat("csv")
                        .setColumnSeparator(",")
                        .build();
                try {
                    streamLoadResult = streamLoad.run(dataList, dorisContentParams, build);
                } catch (StreamLoadException e) {
                    throw new RuntimeException(e);
                }
                dataList.clear();
                log.info(streamLoadResult.toString());
            }
        }
        long endTime = System.currentTimeMillis();
        if (!"Success".equals(streamLoadResult.getStatus())) {
            throw new RuntimeException(streamLoadResult.toString());
        }
        return String.format("All have been successfully imported, a total of %s items, " +
                "and the cumulative time taken is %s Ms", dataSize, endTime - startTime);
    }
}
