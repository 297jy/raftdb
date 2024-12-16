/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zhuanyi.jraftdb.engine.db;


import org.zhuanyi.jraftdb.engine.api.DB;
import org.zhuanyi.jraftdb.engine.api.DBFactory;
import org.zhuanyi.jraftdb.engine.option.Options;
import org.zhuanyi.jraftdb.engine.utils.FileUtils;

import java.io.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class DBFactoryImpl
        implements DBFactory {

    public static final String VERSION;

    static {
        String v = "unknown";
        InputStream is = DBFactoryImpl.class.getResourceAsStream("version.txt");
        try {
            v = new BufferedReader(new InputStreamReader(is, UTF_8)).readLine();
        } catch (Throwable e) {
        } finally {
            try {
                is.close();
            } catch (Throwable e) {
            }
        }
        VERSION = v;
    }

    public static final DBFactoryImpl factory = new DBFactoryImpl();

    @Override
    public DB open(File path, Options options)
            throws IOException {
        return new DbImpl(options, path);
    }

    @Override
    public void destroy(File path, Options options)
            throws IOException {
        // TODO: This should really only delete leveldb-created files.
        FileUtils.deleteRecursively(path);
    }

    @Override
    public void repair(File path, Options options)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return String.format("iq80 leveldb version %s", VERSION);
    }

    public static byte[] bytes(String value) {
        return (value == null) ? null : value.getBytes(UTF_8);
    }

    public static String asString(byte[] value) {
        return (value == null) ? null : new String(value, UTF_8);
    }
}
