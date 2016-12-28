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

    /**
     * @param addr Start address. 
     * @param off Offset.
     * @return Byte value from given address.
     */
    public static byte getByte(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return unsafe.getByte(addr + off);
    }

    /**
     * @param addr Start address.
     * @param off Offset.
     * @param len Bytes length.
     * @return Bytes from given address.
     */
    public static byte[] getBytes(long addr, int off, int len) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert len >= 0;

        byte[] bytes = new byte[len];

        unsafe.copyMemory(null, addr + off, bytes, GridUnsafe.BYTE_ARR_OFF, len);

        return bytes;
    }

    public static void getBytes(long srcAddr, int srcOff, byte[] dst, int dstOff, int len) {
        assert srcAddr > 0;
        assert srcOff > 0;
        assert dst != null;
        assert dstOff >= 0;
        assert len >= 0;

        unsafe.copyMemory(null, srcAddr + srcOff, dst, GridUnsafe.BYTE_ARR_OFF + dstOff, len);
    }

    public static short getShort(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return unsafe.getShort(addr + off);
    }

    public static int getInt(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return unsafe.getInt(addr + off);
    }

    public static long getLong(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return unsafe.getLong(addr + off);
    }

    public static void putBytes(long addr, int off, byte[] bytes) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert bytes != null;

        unsafe.copyMemory(bytes, GridUnsafe.BYTE_ARR_OFF, null, addr + off, bytes.length);
    }

    public static void putBytes(long addr, int off, byte[] bytes, int bytesOff) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert bytes != null;
        assert bytesOff >= 0 && bytesOff < bytes.length : bytesOff;

        unsafe.copyMemory(bytes, GridUnsafe.BYTE_ARR_OFF + bytesOff, null, addr + off, bytes.length - bytesOff);
    }

    public static void putByte(long addr, int off, byte v) {
        assert addr > 0 : addr;
        assert off >= 0;

        unsafe.putByte(addr + off, v);
    }

    public static void putShort(long addr, int off, short v) {
        assert addr > 0 : addr;
        assert off >= 0;

        unsafe.putShort(addr + off, v);
    }

    public static void putInt(long addr, int off, int v) {
        assert addr > 0 : addr;
        assert off >= 0;

        unsafe.putInt(addr + off, v);
    }

    public static void putLong(long addr, int off, long v) {
        assert addr > 0 : addr;
        assert off >= 0;

        unsafe.putLong(addr + off, v);
    }
}
