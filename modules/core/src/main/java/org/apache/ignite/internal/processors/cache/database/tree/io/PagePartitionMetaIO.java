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
 *
 */

package org.apache.ignite.internal.processors.cache.database.tree.io;

import org.apache.ignite.internal.pagemem.PageUtils;

/**
 *
 */
public class PagePartitionMetaIO extends PageMetaIO {
    /** */
    private static final int SIZE_OFF = PageMetaIO.END_OF_PAGE_META;

    /** */
    private static final int UPDATE_CNTR_OFF = SIZE_OFF + 8;

    /** */
    private static final int GLOBAL_RMV_ID_OFF = UPDATE_CNTR_OFF + 8;

    /** */
    private static final int PARTITION_STATE_OFF = GLOBAL_RMV_ID_OFF + 8;

    /** */
    public static final IOVersions<PagePartitionMetaIO> VERSIONS = new IOVersions<>(
        new PagePartitionMetaIO(1)
    );

    /** {@inheritDoc} */
    @Override public void initNewPage(long buf, long pageId) {
        super.initNewPage(buf, pageId);

        setSize(buf, 0);
        setUpdateCounter(buf, 0);
        setGlobalRemoveId(buf, 0);
        setPartitionState(buf, (byte)-1);
    }

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIO(int ver) {
        super(T_PART_META, ver);
    }

    /**
     * @param buf Buffer.
     * @return Partition size.
     */
    public long getSize(long buf) {
        return PageUtils.getLong(buf, SIZE_OFF);
    }

    /**
     * @param buf Buffer.
     * @param size Partition size.
     */
    public void setSize(long buf, long size) {
        PageUtils.putLong(buf, SIZE_OFF, size);
    }

    /**
     * @param buf Buffer.
     * @return Partition update counter.
     */
    public long getUpdateCounter(long buf) {
        return PageUtils.getLong(buf, UPDATE_CNTR_OFF);
    }

    /**
     * @param buf Buffer.
     * @param cntr Partition update counter.
     */
    public void setUpdateCounter(long buf, long cntr) {
        PageUtils.putLong(buf, UPDATE_CNTR_OFF, cntr);
    }

    /**
     * @param buf Buffer.
     * @return Global remove ID.
     */
    public long getGlobalRemoveId(long buf) {
        return PageUtils.getLong(buf, GLOBAL_RMV_ID_OFF);
    }

    /**
     * @param buf Buffer.
     * @param rmvId Global remove ID.
     */
    public void setGlobalRemoveId(long buf, long rmvId) {
        PageUtils.putLong(buf, GLOBAL_RMV_ID_OFF, rmvId);
    }

    /**
     * @param buf Buffer.
     */
    public byte getPartitionState(long buf) {
        return PageUtils.getByte(buf, PARTITION_STATE_OFF);
    }

    /**
     * @param buf
     * @param state
     */
    public void setPartitionState(long buf, byte state) {
        PageUtils.putByte(buf, PARTITION_STATE_OFF, state);
    }
}
