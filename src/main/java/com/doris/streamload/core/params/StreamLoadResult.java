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

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class StreamLoadResult {
    private Long txnId;
    private String label;
    private String twoPhaseCommit;
    private String status;
    private String message;
    private Integer numberTotalRows;
    private Integer numberLoadedRows;
    private Integer numberFilteredRows;
    private Integer numberUnselectedRows;
    private Long loadBytes;
    private Long loadTimeMs;
    private Long beginTxnTimeMs;
    private Long streamLoadPutTimeMs;
    private Long readDataTimeMs;
    private Long writeDataTimeMs;
    private Long commitAndPublishTimeMs;
    private String errorURL;

    @Override
    public String toString() {
        return "{" +
                "txnId=" + txnId +
                ", label='" + label + '\'' +
                ", twoPhaseCommit='" + twoPhaseCommit + '\'' +
                ", status='" + status + '\'' +
                ", message='" + message + '\'' +
                ", numberTotalRows=" + numberTotalRows +
                ", numberLoadedRows=" + numberLoadedRows +
                ", numberFilteredRows=" + numberFilteredRows +
                ", numberUnselectedRows=" + numberUnselectedRows +
                ", loadBytes=" + loadBytes +
                ", loadTimeMs=" + loadTimeMs +
                ", beginTxnTimeMs=" + beginTxnTimeMs +
                ", streamLoadPutTimeMs=" + streamLoadPutTimeMs +
                ", readDataTimeMs=" + readDataTimeMs +
                ", writeDataTimeMs=" + writeDataTimeMs +
                ", commitAndPublishTimeMs=" + commitAndPublishTimeMs +
                ", errorUrl=" + errorURL +
                '}';
    }
}
