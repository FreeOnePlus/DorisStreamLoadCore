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
package com.doris.streamload.core;

import com.doris.streamload.core.params.StreamLoadParams;
import com.doris.streamload.core.params.DorisContentParams;
import com.doris.streamload.core.params.StreamLoadResult;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
@Repository
public class StreamLoad {
    private StreamLoadResult streamLoadResult;
    private IConvertor convertor;
    // Create HttpClientBuilder And Open 307
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });
    // Create Gson Object And Convert from PascalCase to camelCase.
    private final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
            .create();

    public StreamLoad(IConvertor convertor){
        this.convertor = convertor;
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public StreamLoadResult run(Object data, DorisContentParams dorisContentParams, StreamLoadParams streamLoadParams){
        switch (streamLoadParams.getFormat()){
            case "csv":
                String csv = convertor.convertorToCsv(data);
                return doLoad(csv, dorisContentParams,streamLoadParams);
            case "csv_with_names":
                String csvWithNames = convertor.convertorToCsvWithNames(data);
                return doLoad(csvWithNames, dorisContentParams,streamLoadParams);
            case "csv_with_names_and_types":
                String csvWithNamesAndTypes = convertor.convertorToCsvWithNamesAndTypes(data);
                return doLoad(csvWithNamesAndTypes, dorisContentParams,streamLoadParams);
            case "json":
                String json = convertor.convertorToJson(data);
                return doLoad(json, dorisContentParams,streamLoadParams);
            default:
                StreamLoadResult loadResult = new StreamLoadResult();
                loadResult.setStatus("Error");
                loadResult.setMessage("Dont have this data format!");
                return streamLoadResult;
        }
    }

    private StreamLoadResult doLoad(Object data, DorisContentParams dorisContentParams, StreamLoadParams streamLoadParams) {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(String.format("http://%s:%s/api/%s/%s/_stream_load",
                    dorisContentParams.getHost(),
                    dorisContentParams.getHttpPort(),
                    dorisContentParams.getDatabase(),
                    dorisContentParams.getTable()));
            StringEntity entity = new StringEntity(data.toString(), "UTF-8");
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(dorisContentParams.getUserName(), dorisContentParams.getPassword()));
            for (Map.Entry<String, String> paramsEntry : streamLoadParams.getParamMap().entrySet()) {
                put.setHeader(paramsEntry.getKey(), paramsEntry.getValue());
            }
            put.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(put)) {
                if (response.getEntity() != null) {
                    String loadResult = EntityUtils.toString(response.getEntity());
                    streamLoadResult = gson.fromJson(loadResult, StreamLoadResult.class);
                }
                final int statusCode = response.getStatusLine().getStatusCode();
                // statusCode 200 just indicates that doris be service is ok, not stream load
                // you should see the output data to find whether stream load is success
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Stream load failed, statusCode=%s load result=%s", statusCode, streamLoadResult.toString()));
                }
            }
            return streamLoadResult;
        } catch (IOException e) {
            // log.Error(streamLoadResult.toString());
            throw new RuntimeException(e);
        }
    }
}

