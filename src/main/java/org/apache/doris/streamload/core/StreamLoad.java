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
package org.apache.doris.streamload.core;

import org.apache.doris.streamload.core.exception.StreamLoadException;
import org.apache.doris.streamload.core.params.DorisContentParams;
import org.apache.doris.streamload.core.input.StreamLoadInputStream;
import org.apache.doris.streamload.core.params.StreamLoadParams;
import org.apache.doris.streamload.core.params.StreamLoadResult;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class StreamLoad {
    private static final Logger log = LoggerFactory.getLogger(StreamLoad.class);
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

    public StreamLoadResult run(Object data
            , DorisContentParams dorisContentParams
            , StreamLoadParams streamLoadParams) throws StreamLoadException {
        String doLoadData;
        switch (streamLoadParams.getFormat()){
            case "csv":
                doLoadData = convertor.convertorToCsv(data);
                break;
            case "csv_with_names":
                doLoadData = convertor.convertorToCsvWithNames(data);
                break;
            case "csv_with_names_and_types":
                doLoadData = convertor.convertorToCsvWithNamesAndTypes(data);
                break;
            case "json":
                doLoadData = convertor.convertorToJson(data);
                break;
            default:
                StreamLoadResult loadResult = new StreamLoadResult();
                loadResult.setStatus("Error");
                loadResult.setMessage("Dont have this data format!");
                return streamLoadResult;
        }
        return doLoad(doLoadData, dorisContentParams,streamLoadParams);
    }

    private StreamLoadResult doLoad(Object data
            , DorisContentParams dorisContentParams
            , StreamLoadParams streamLoadParams) throws StreamLoadException {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(String.format("http://%s:%s/api/%s/%s/_stream_load",
                    dorisContentParams.getHost(),
                    dorisContentParams.getHttpPort(),
                    dorisContentParams.getDatabase(),
                    dorisContentParams.getTable()));
            // because only support "csv" or "json" type with now.
            // Here, because inputSteam currently has certain problems importing JSON type data,
            // StringEntity is used, which will be unified in the future.
            if (streamLoadParams.getFormat().matches("csv")){
                StreamLoadInputStream streamLoadInputStream = new StreamLoadInputStream(data.toString());
                InputStreamEntity inputStreamEntity = new InputStreamEntity(streamLoadInputStream
                        ,ContentType.create("text/plain", StandardCharsets.UTF_8));
                put.setEntity(inputStreamEntity);
            }else if(streamLoadParams.getFormat().matches("json")){
                put.setEntity(new StringEntity(data.toString(), "UTF-8"));
            } else {
                throw new StreamLoadException("This data type is not yet supported by the InputStream method.");
            }

            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(dorisContentParams.getUserName()
                    , dorisContentParams.getPassword()));
            for (Map.Entry<String, String> paramsEntry : streamLoadParams.getParamMap().entrySet()) {
                put.setHeader(paramsEntry.getKey(), paramsEntry.getValue());
            }

            try (CloseableHttpResponse response = client.execute(put)) {
                if (response.getEntity() != null) {
                    String loadResult = EntityUtils.toString(response.getEntity());
                    streamLoadResult = gson.fromJson(loadResult, StreamLoadResult.class);
                }
                final int statusCode = response.getStatusLine().getStatusCode();
                // statusCode 200 just indicates that doris be service is ok, not stream load
                // you should see the output data to find whether stream load is success
                if (statusCode != 200) {
                    throw new StreamLoadException(
                            String.format("Stream load failed, statusCode=%s load result=%s"
                                    , statusCode, streamLoadResult.toString()));
                }
            }
            return streamLoadResult;
        } catch (IOException | StreamLoadException e) {
            log.error(streamLoadResult.toString());
            throw new StreamLoadException(e);
        }
    }
}

