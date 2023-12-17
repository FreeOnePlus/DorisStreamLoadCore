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
package org.doris.streamload.core.params;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The necessary parameters for using StreamLoad to connect to
 * Doris can be obtained from the ‘DorisConfig’ configuration class.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DorisContentParams {
    // FE or BE Host
    private String host;
    // If Host Param is FE Host, Please use fe-http-port, Default value 8030.
    // If Host Param is BE Host, Please use be-http-port, Default value 8040.
    private int httpPort;
    // Write to target database
    private String database;
    // Write to target table
    private String table;
    // Doris login username
    private String userName;
    // Doris login password
    private String password;
}
