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
package com.doris.streamload.demo.services.impl.convertor;

import com.doris.streamload.demo.beans.DataBean;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.doris.streamload.core.IConvertor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This implementation class needs to implement specific logic according to the data type.
 * It belongs to the data processing implementation class.
 * It completes different data processing logic according to the different interfaces implemented,
 * and then loads the data in the StreamLoad core code.
 */
@Service
public class IConvertorImpl implements IConvertor {
    /**
     * Construct a Gson object and assign the LOWER_CASE_WITH_UNDERSCORES
     * parameter to it to ensure that when it converts the Bean class,
     * the camel case naming rule can be automatically changed to the
     * underscore naming rule.
     */
    private Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    @Override
    public String convertorToCsv(Object input) {
        if (input instanceof List) {
            List list = (List) input;
            if (list.size() > 0 && list.get(0) instanceof DataBean){
                List<DataBean> dataBeanList = (List<DataBean>) input;
                return dataBeanList.stream()
                        .map(DataBean::toString)
                        .collect(Collectors.joining("\n"));
            }
            return null;
        }
        return null;
    }

    @Override
    public String convertorToCsvWithNames(Object input) {
        return null;
    }

    @Override
    public String convertorToCsvWithNamesAndTypes(Object input) {
        return null;
    }

    @Override
    public String convertorToJson(Object input) {
        if (input instanceof List) {
            List list = (List) input;
            if (list.size() > 0 && list.get(0) instanceof DataBean){
                List<DataBean> dataBeanList = (List<DataBean>) input;
                return gson.toJson(dataBeanList);
            }
            return null;
        }
        return null;
    }

}
