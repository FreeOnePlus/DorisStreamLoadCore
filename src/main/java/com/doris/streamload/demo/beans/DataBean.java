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
package com.doris.streamload.demo.beans;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * CREATE TABLE
 *   `app_log` (
 *     `event_id` bigint(20) NULL COMMENT '埋点事件ID',
 *     `user_id` varchar(255) NULL COMMENT '用户ID',
 *     `event_type` varchar(255) NULL COMMENT '事件类型',
 *     `timestamp` datetimev2(0) NULL COMMENT '时间戳',
 *     `page_name` varchar(255) NULL COMMENT '页面名称',
 *     `action_details` varchar(*) NULL COMMENT '行为详情',
 *     `device_type` varchar(255) NULL COMMENT '设备类型',
 *     `os_version` varchar(255) NULL COMMENT '操作系统版本',
 *     `app_version` varchar(255) NULL COMMENT '应用版本',
 *     `geo_location` varchar(255) NULL COMMENT '地理位置',
 *     `network_type` varchar(255) NULL COMMENT '网络类型'
 *   ) ENGINE = OLAP COMMENT 'OLAP' DISTRIBUTED BY HASH(`event_id`) BUCKETS 10 PROPERTIES (
 *     "file_cache_ttl_seconds" = "0",
 *     "light_schema_change" = "true",
 *     "enable_duplicate_without_keys_by_default" = "true"
 *   );
 */

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DataBean {
    private Long eventId; // 埋点事件ID
    private String userId; // 用户ID
    private String eventType; // 事件类型
    private String timestamp; // 时间戳
    private String pageName; // 页面名称
    private String actionDetails; // 行为详情
    private String deviceType; // 设备类型
    private String osVersion; // 操作系统版本
    private String appVersion; // 应用版本
    private String geoLocation; // 地理位置
    private String networkType; // 网络类型

    @Override
    public String toString() {
        return eventId +
                "," + userId +
                "," + eventType +
                "," + timestamp +
                "," + pageName +
                "," + actionDetails +
                "," + deviceType +
                "," + osVersion +
                "," + appVersion +
                "," + geoLocation +
                "," + networkType ;
    }
}

