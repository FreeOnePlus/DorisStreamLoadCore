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
package com.doris.streamload.core.input;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

public class StreamLoadInputStream extends InputStream {
    private InputStream innerStream; //Inner InputStream object
    private byte[] buffer; // Buffer used to store temporary data
    private int position; //The current position in the buffer
    private int limit; //The upper limit of valid data in the buffer

    // Constructor to receive byte array
    public StreamLoadInputStream(byte[] data) {
        this.buffer = data;
        this.position = 0;
        this.limit = data.length;
    }

    // Constructor to receive string
    public StreamLoadInputStream(String data) {
        this(data.getBytes(StandardCharsets.UTF_8));
    }

    //Constructor that receives integers
    public StreamLoadInputStream(int data) {
        this(Integer.toString(data));
    }

    // Constructor to receive floating point numbers
    public StreamLoadInputStream(float data) {
        this(Float.toString(data));
    }

    //Constructor that receives a boolean value
    public StreamLoadInputStream(boolean data) {
        this(Boolean.toString(data));
    }

    //Constructor that receives File object
    public StreamLoadInputStream(File file) throws IOException {
        this.innerStream = new FileInputStream(file);
    }

    //Constructor that receives the URL object
    public StreamLoadInputStream(URL url) throws IOException {
        URLConnection connection = url.openConnection();
        this.innerStream = connection.getInputStream();
    }

    //General reading method
    @Override
    public int read() throws IOException {
        if (innerStream != null) {
            return innerStream.read();
        }
        if (position < limit) {
            return buffer[position++] & 0xff;
        }
        return -1; // End of reading
    }

    // Rewrite the read(byte[] b, int off, int len) method
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (innerStream != null) {
            return innerStream.read(b, off, len);
        }
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (position >= limit) {
            return -1; // End of reading
        }
        int available = limit - position;
        if (len > available) {
            len = available;
        }
        System.arraycopy(buffer, position, b, off, len);
        position += len;
        return len;
    }

    // Override available() method
    @Override
    public int available() throws IOException {
        if (innerStream != null) {
            return innerStream.available();
        }
        return limit - position;
    }

    // Method to close the stream
    @Override
    public void close() throws IOException {
        if (innerStream != null) {
            innerStream.close();
        }
        buffer = null;
        position = -1;
        limit = 0;
    }
}

