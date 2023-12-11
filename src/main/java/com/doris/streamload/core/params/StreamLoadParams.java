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
package com.doris.streamload.core.params;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class StreamLoadParams {
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

    // JSON Parameter
    private String stripOuterArray = "false";
    private String fuzzyParse = "false";

    // CSV Parameter
    private String columnSeparator = "\t";
    private String lineDelimiter = "\n";
    private String enclose = "";
    private String escape = "";

    // CSV Constructor
    public StreamLoadParams(String label, String format, String strictMode, String maxFilterRatio, String where, String partitions, String columns, String execMemLimit, String partialColumns, String mergeType, String twoPhaseCommit, String enableProfile, String stripOuterArray, String fuzzyParse) {
        this.label = label;
        this.format = format;
        this.strictMode = strictMode;
        this.maxFilterRatio = maxFilterRatio;
        this.where = where;
        this.partitions = partitions;
        this.columns = columns;
        this.execMemLimit = execMemLimit;
        this.partialColumns = partialColumns;
        this.mergeType = mergeType;
        this.twoPhaseCommit = twoPhaseCommit;
        this.enableProfile = enableProfile;
        this.stripOuterArray = stripOuterArray;
        this.fuzzyParse = fuzzyParse;
    }

    // JSON Constructor
    public StreamLoadParams(String label, String format, String strictMode, String maxFilterRatio, String where, String partitions, String columns, String execMemLimit, String partialColumns, String mergeType, String twoPhaseCommit, String enableProfile, String columnSeparator, String lineDelimiter, String enclose, String escape) {
        this.label = label;
        this.format = format;
        this.strictMode = strictMode;
        this.maxFilterRatio = maxFilterRatio;
        this.where = where;
        this.partitions = partitions;
        this.columns = columns;
        this.execMemLimit = execMemLimit;
        this.partialColumns = partialColumns;
        this.mergeType = mergeType;
        this.twoPhaseCommit = twoPhaseCommit;
        this.enableProfile = enableProfile;
        this.columnSeparator = columnSeparator;
        this.lineDelimiter = lineDelimiter;
        this.enclose = enclose;
        this.escape = escape;
    }

    public Map<String,String> getParamMap(){
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
        paramList.put("strip_outer_array", getStripOuterArray());
        paramList.put("fuzzy_parse", getFuzzyParse());
        paramList.put("column_separator", getColumnSeparator());
        paramList.put("line_delimiter", getLineDelimiter());
        paramList.put("enclose", getEnclose());
        paramList.put("escape", getEscape());
        return paramList;
    }

    @Override
    public String toString() {
        return "{" +
                "format='" + format + '\'' +
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
                ", stripOuterArray='" + stripOuterArray + '\'' +
                ", fuzzyParse='" + fuzzyParse + '\'' +
                ", separator='" + columnSeparator + '\'' +
                ", lineDelimiter='" + lineDelimiter + '\'' +
                ", enclose='" + enclose + '\'' +
                ", escape='" + escape + '\'' +
                '}';
    }
}
