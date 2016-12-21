/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagemem;

import org.apache.ignite.internal.util.GridUnsafe;
import sun.misc.Unsafe;

/**
 *
 */
@SuppressWarnings("deprecation")
public class PageUtils {
    /** */
    private static final Unsafe unsafe = GridUnsafe.UNSAFE;

    public static byte getByte(long buf, int off) {
        return unsafe.getByte(buf, off);
    }

    public static short getShort(long buf, int off) {
        return unsafe.getShort(buf, off);
    }

    public static int getInt(long buf, int off) {
        return unsafe.getInt(buf, off);
    }

    public static long getLong(long buf, int off) {
        return unsafe.getLong(buf, off);
    }

    public static void putBytes(long buf, int off, byte[] bytes) {
        unsafe.copyMemory(bytes, GridUnsafe.BYTE_ARR_OFF, null, buf + off, bytes.length);
    }

    public static void putByte(long buf, int off, byte v) {
        unsafe.putByte(buf, off, v);
    }

    public static void putShort(long buf, int off, short v) {
        unsafe.putShort(buf, off, v);
    }

    public static void putInt(long buf, int off, int v) {
        unsafe.putInt(buf, off, v);
    }

    public static void putLong(long buf, int off, long v) {
        unsafe.putLong(buf, off, v);
    }
}
