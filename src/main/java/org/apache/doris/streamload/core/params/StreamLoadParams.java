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
package org.apache.doris.streamload.core.params;

import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
public class StreamLoadParams {
    // Public Parameter
    private String label;
    private String format; // enums: csv,json,csv_with_names,csv_with_names_and_types,parquet,orc
    private String strictMode;
    private String maxFilterRatio;
    private String where;
    private String partitions;
    private String columns;
    private String execMemLimit;
    private String partialColumns;
    private String mergeType; // enums: APPEND、DELETE、MERGE
    private String twoPhaseCommit;
    private String enableProfile;
    private String timeout;

    // JSON Parameter
    private String stripOuterArray;
    private String fuzzyParse;
    private String jsonpaths;
    private String readJsonByLine;
    private String jsonRoot;

    // CSV Parameter
    private String columnSeparator;
    private String lineDelimiter;
    private String enclose;
    private String escape;

    public Map<String, String> getParamMap() {
        HashMap<String, String> paramList = new HashMap<>();
        paramList.put("label", getLabel());
        paramList.put("format", getFormat());
        paramList.put("strict_mode", getStrictMode());
        paramList.put("max_filter_ratio", getMaxFilterRatio());
        paramList.put("where", getWhere());
        paramList.put("partitions", getPartitions());
        paramList.put("columns", getColumns());
        paramList.put("exec_mem_limit", getExecMemLimit());
        paramList.put("partial_columns", getPartialColumns());
        paramList.put("merge_type", getMergeType());
        paramList.put("two_phase_commit", getTwoPhaseCommit());
        paramList.put("enable_profile", getEnableProfile());
        paramList.put("timeout", getTimeout());
        paramList.put("strip_outer_array", getStripOuterArray());
        paramList.put("fuzzy_parse", getFuzzyParse());
        paramList.put("jsonpaths", getJsonpaths());
        paramList.put("json_root", getJsonRoot());
        paramList.put("read_json_by_line", getReadJsonByLine());
        paramList.put("column_separator", getColumnSeparator());
        paramList.put("line_delimiter", getLineDelimiter());
        paramList.put("enclose", getEnclose());
        paramList.put("escape", getEscape());
        return paramList;
    }

    private StreamLoadParams(Builder builder) {
        this.label = builder.label;
        this.format = builder.format;
        this.strictMode = builder.strictMode;
        this.maxFilterRatio = builder.maxFilterRatio;
        this.where = builder.where;
        this.partitions = builder.partitions;
        this.columns = builder.columns;
        this.execMemLimit = builder.execMemLimit;
        this.partialColumns = builder.partialColumns;
        this.mergeType = builder.mergeType;
        this.twoPhaseCommit = builder.twoPhaseCommit;
        this.enableProfile = builder.enableProfile;
        this.timeout = builder.timeout;
        this.stripOuterArray = builder.stripOuterArray;
        this.fuzzyParse = builder.fuzzyParse;
        this.jsonpaths = builder.jsonpaths;
        this.readJsonByLine = builder.readJsonByLine;
        this.jsonRoot = builder.jsonRoot;
        this.columnSeparator = builder.columnSeparator;
        this.lineDelimiter = builder.lineDelimiter;
        this.enclose = builder.enclose;
        this.escape = builder.escape;
    }

    public static class Builder {
        // Public Parameter
        private String label = UUID.randomUUID().toString();
        private String format = "csv"; // enums: csv,json,csv_with_names,csv_with_names_and_types,parquet,orc
        private String strictMode = "false";
        private String maxFilterRatio = "0";
        private String where = "";
        private String partitions = "";
        private String columns = "";
        private String execMemLimit = "2147483648";
        private String partialColumns = "false";
        private String mergeType = "APPEND"; // enums: APPEND、DELETE、MERGE
        private String twoPhaseCommit = "false";
        private String enableProfile = "false";
        private String timeout = "";

        // JSON Parameter
        private String stripOuterArray = "false";
        private String fuzzyParse = "false";
        private String jsonpaths = "";
        private String readJsonByLine = "";
        private String jsonRoot = "";

        // CSV Parameter
        private String columnSeparator = "\t";
        private String lineDelimiter = "\n";
        private String enclose = "";
        private String escape = "";

        public Builder setLabel(String label){
            this.label = label;
            return this;
        }

        public Builder setFormat(String format){
            this.format = format;
            return this;
        }

        public Builder setStrictMode(String strictMode){
            this.strictMode = strictMode;
            return this;
        }
        public Builder setMaxFilterRatio(String maxFilterRatio){
            this.maxFilterRatio = maxFilterRatio;
            return this;
        }
        public Builder setWhere(String where){
            this.where = where;
            return this;
        }
        public Builder setPartitions(String partitions){
            this.partitions = partitions;
            return this;
        }
        public Builder setColumns(String columns){
            this.columns = columns;
            return this;
        }
        public Builder setExecMemLimit(String execMemLimit){
            this.execMemLimit = execMemLimit;
            return this;
        }
        public Builder setPartialColumns(String partialColumns){
            this.partialColumns = partialColumns;
            return this;
        }
        public Builder setMergeType(String mergeType){
            this.mergeType = mergeType;
            return this;
        }

        public Builder setTwoPhaseCommit(String twoPhaseCommit){
            this.twoPhaseCommit = twoPhaseCommit;
            return this;
        }
        public Builder setEnableProfile(String enableProfile){
            this.enableProfile = enableProfile;
            return this;
        }
        public Builder setTimeout(String timeout){
            this.timeout = timeout;
            return this;
        }
        public Builder setStripOuterArray(String stripOuterArray){
            this.stripOuterArray = stripOuterArray;
            return this;
        }
        public Builder setFuzzyParse(String fuzzyParse){
            this.fuzzyParse = fuzzyParse;
            return this;
        }
        public Builder setJsonpaths(String jsonpaths){
            this.jsonpaths = jsonpaths;
            return this;
        }
        public Builder setReadJsonByLine(String readJsonByLine){
            this.readJsonByLine = readJsonByLine;
            return this;
        }
        public Builder setJsonRoot(String jsonRoot){
            this.jsonRoot = jsonRoot;
            return this;
        }
        public Builder setColumnSeparator(String columnSeparator){
            this.columnSeparator = columnSeparator;
            return this;
        }
        public Builder setLineDelimiter(String lineDelimiter){
            this.lineDelimiter = lineDelimiter;
            return this;
        }
        public Builder setEnclose(String enclose){
            this.enclose = enclose;
            return this;
        }
        public Builder setEscape(String escape){
            this.escape = escape;
            return this;
        }

        public StreamLoadParams build(){
            return new StreamLoadParams(this);
        }

    }

    @Override
    public String toString() {
        return "{" +
                "label='" + label + '\'' +
                ", format='" + format + '\'' +
                ", strictMode='" + strictMode + '\'' +
                ", maxFilterRatio='" + maxFilterRatio + '\'' +
                ", where='" + where + '\'' +
                ", partitions='" + partitions + '\'' +
                ", columns='" + columns + '\'' +
                ", execMemLimit='" + execMemLimit + '\'' +
                ", partialColumns='" + partialColumns + '\'' +
                ", mergeType='" + mergeType + '\'' +
                ", twoPhaseCommit='" + twoPhaseCommit + '\'' +
                ", enableProfile='" + enableProfile + '\'' +
                ", timeout='" + timeout + '\'' +
                ", stripOuterArray='" + stripOuterArray + '\'' +
                ", fuzzyParse='" + fuzzyParse + '\'' +
                ", jsonpaths='" + jsonpaths + '\'' +
                ", readJsonByLine='" + readJsonByLine + '\'' +
                ", columnSeparator='" + columnSeparator + '\'' +
                ", lineDelimiter='" + lineDelimiter + '\'' +
                ", enclose='" + enclose + '\'' +
                ", escape='" + escape + '\'' +
                '}';
    }
}
