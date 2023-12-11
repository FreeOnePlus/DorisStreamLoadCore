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

public class DataValueEnums {
    // 用户ID枚举值
    public static final String[] USER_IDS = {
            "user001", "user002", "user003", "user004", "user005",
            "user006", "user007", "user008", "user009", "user010"
    };

    // 事件类型枚举值
    public static final String[] EVENT_TYPES = {
            "click", "view", "purchase", "login", "logout",
            "signup", "share", "comment", "like", "navigate"
    };

    // 页面名称枚举值
    public static final String[] PAGE_NAMES = {
            "homepage", "profile", "settings", "cart", "checkout",
            "search", "category", "product", "about", "contact"
    };

    // 行为详情枚举值
    public static final String[] ACTION_DETAILS = {
            "button_click", "image_view", "item_add_to_cart", "form_submit", "video_play",
            "link_click", "item_remove_from_cart", "quantity_change", "filter_apply", "sort_select"
    };

    // 设备类型枚举值
    public static final String[] DEVICE_TYPES = {
            "smartphone", "tablet", "desktop", "laptop", "smartwatch",
            "e-reader", "gaming_console", "smart_tv", "vr_headset", "wearable"
    };

    // 操作系统版本枚举值
    public static final String[] OS_VERSIONS = {
            "Android_11", "Android_12", "iOS_14", "iOS_15", "Windows_10",
            "Windows_11", "macOS_Monterey", "macOS_Big_Sur", "Linux_Ubuntu", "Linux_Fedora"
    };

    // 应用版本枚举值
    public static final String[] APP_VERSIONS = {
            "1.0.0", "1.0.1", "1.1.0", "1.2.0", "2.0.0",
            "2.1.0", "2.2.0", "3.0.0", "3.1.0", "3.1.1"
    };

    // 地理位置枚举值
    public static final String[] GEO_LOCATIONS = {
            "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu",
            "Hangzhou", "Wuhan", "Chongqing", "Nanjing", "Xi'an"
    };

    // 网络类型枚举值
    public static final String[] NETWORK_TYPES = {
            "4G", "5G", "WiFi", "Ethernet", "LTE",
            "WCDMA", "HSPA", "UMTS", "CDMA", "GSM"
    };
}

