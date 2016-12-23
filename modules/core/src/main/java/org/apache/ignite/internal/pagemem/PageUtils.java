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
        assert buf > 0 : buf;
        assert off >= 0;

        return unsafe.getByte(buf + off);
    }

    public static byte[] getBytes(long buf, int off, int len) {
        assert buf > 0 : buf;
        assert off >= 0;
        assert len >= 0;

        byte[] bytes = new byte[len];

        unsafe.copyMemory(null, buf + off, bytes, GridUnsafe.BYTE_ARR_OFF, len);

        return bytes;
    }

    public static void getBytes(long src, int srcOff, byte[] dst, int dstOff, int len) {
        assert src > 0;
        assert srcOff > 0;
        assert dst != null;
        assert dstOff >= 0;
        assert len >= 0;

        unsafe.copyMemory(null, src + srcOff, dst, GridUnsafe.BYTE_ARR_OFF + dstOff, len);
    }

    public static short getShort(long buf, int off) {
        assert buf > 0 : buf;
        assert off >= 0;

        return unsafe.getShort(buf + off);
    }

    public static int getInt(long buf, int off) {
        assert buf > 0 : buf;
        assert off >= 0;

        return unsafe.getInt(buf + off);
    }

    public static long getLong(long buf, int off) {
        assert buf > 0 : buf;
        assert off >= 0;

        return unsafe.getLong(buf + off);
    }

    public static void putBytes(long buf, int off, byte[] bytes) {
        assert buf > 0 : buf;
        assert off >= 0;
        assert bytes != null;

        unsafe.copyMemory(bytes, GridUnsafe.BYTE_ARR_OFF, null, buf + off, bytes.length);
    }

    public static void putByte(long buf, int off, byte v) {
        assert buf > 0 : buf;
        assert off >= 0;

        unsafe.putByte(buf + off, v);
    }

    public static void putShort(long buf, int off, short v) {
        assert buf > 0 : buf;
        assert off >= 0;

        unsafe.putShort(buf + off, v);
    }

    public static void putInt(long buf, int off, int v) {
        assert buf > 0 : buf;
        assert off >= 0;

        unsafe.putInt(buf + off, v);
    }

    public static void putLong(long buf, int off, long v) {
        assert buf > 0 : buf;
        assert off >= 0;

        unsafe.putLong(buf + off, v);
    }
}
