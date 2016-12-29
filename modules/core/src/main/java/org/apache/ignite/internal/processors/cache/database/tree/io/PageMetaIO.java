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

package org.apache.ignite.internal.processors.cache.database.tree.io;

import org.apache.ignite.internal.pagemem.PageUtils;

/**
 *
 */
public class PageMetaIO extends PageIO {
    /** */
    private static final int METASTORE_ROOT_OFF = PageIO.COMMON_HEADER_END;

    /** */
    private static final int REUSE_LIST_ROOT_OFF = METASTORE_ROOT_OFF + 8;

    /** Last successful backup id offset. */
    private static final int LAST_SUCCESSFUL_BACKUP_ID_OFF = REUSE_LIST_ROOT_OFF + 8;

    /** Last successful full backup id offset. */
    private static final int LAST_SUCCESSFUL_FULL_BACKUP_ID_OFF = LAST_SUCCESSFUL_BACKUP_ID_OFF + 8;

    /** Next backup id offset. */
    private static final int NEXT_BACKUP_ID_OFF = LAST_SUCCESSFUL_FULL_BACKUP_ID_OFF + 8;

    /** Last allocated index offset. */
    private static final int LAST_ALLOCATED_INDEX_OFF = NEXT_BACKUP_ID_OFF + 8;

    /** Candidate allocated index offset. */
    private static final int CANDIDATE_ALLOCATED_INDEX_OFF = LAST_ALLOCATED_INDEX_OFF + 4;

    /** End of page meta. */
    static final int END_OF_PAGE_META = CANDIDATE_ALLOCATED_INDEX_OFF + 4;

    /** */
    public static final IOVersions<PageMetaIO> VERSIONS = new IOVersions<>(
        new PageMetaIO(1)
    );

    /**
     * @param ver Page format version.
     */
    public PageMetaIO(int ver) {
        super(PageIO.T_META, ver);
    }

    /**
     * @param type Type.
     * @param ver Version.
     */
    protected PageMetaIO(int type, int ver) {
       super(type, ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setTreeRoot(pageAddr, 0);
        setReuseListRoot(pageAddr, 0);
        setLastSuccessfulFullBackupId(pageAddr, 0);
        setLastSuccessfulBackupId(pageAddr, 0);
        setNextBackupId(pageAddr, 1);
        setLastAllocatedIndex(pageAddr, 0);
        setCandidateAllocatedIndex(pageAddr, 0);
    }

    /**
     * @param pageAddr Page address.
     * @return Meta store root page.
     */
    public long getTreeRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, METASTORE_ROOT_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param metastoreRoot metastore root
     */
    public void setTreeRoot(long pageAddr, long metastoreRoot) {
        PageUtils.putLong(pageAddr, METASTORE_ROOT_OFF, metastoreRoot);
    }

    /**
     * @param pageAddr Page address.
     * @return Reuse list root page.
     */
    public long getReuseListRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, REUSE_LIST_ROOT_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param pageId Root page ID.
     */
    public void setReuseListRoot(long pageAddr, long pageId) {
        PageUtils.putLong(pageAddr, REUSE_LIST_ROOT_OFF, pageId);
    }

    /**
     * @param pageAddr Page address.
     * @param lastSuccessfulBackupId Last successful backup id.
     */
    public void setLastSuccessfulBackupId(long pageAddr, long lastSuccessfulBackupId) {
        PageUtils.putLong(pageAddr, LAST_SUCCESSFUL_BACKUP_ID_OFF, lastSuccessfulBackupId);
    }

    /**
     * @param pageAddr Page address.
     */
    public long getLastSuccessfulBackupId(long pageAddr) {
        return PageUtils.getLong(pageAddr, LAST_SUCCESSFUL_BACKUP_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param lastSuccessfulFullBackupId Last successful full backup id.
     */
    public void setLastSuccessfulFullBackupId(long pageAddr, long lastSuccessfulFullBackupId) {
        PageUtils.putLong(pageAddr, LAST_SUCCESSFUL_FULL_BACKUP_ID_OFF, lastSuccessfulFullBackupId);
    }

    /**
     * @param pageAddr Page address.
     */
    public long getLastSuccessfulFullBackupId(long pageAddr) {
        return PageUtils.getLong(pageAddr, LAST_SUCCESSFUL_FULL_BACKUP_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param nextBackupId Next backup id.
     */
    public void setNextBackupId(long pageAddr, long nextBackupId) {
        PageUtils.putLong(pageAddr, NEXT_BACKUP_ID_OFF, nextBackupId);
    }

    /**
     * @param pageAddr Page address.
     * @param lastAllocatedIdx Last allocated index.
     */
    public void setLastAllocatedIndex(long pageAddr, int lastAllocatedIdx) {
        PageUtils.putInt(pageAddr, LAST_ALLOCATED_INDEX_OFF, lastAllocatedIdx);
    }

    /**
     * @param pageAddr Page address.
     */
    public int getLastAllocatedIndex(long pageAddr) {
        return PageUtils.getInt(pageAddr, LAST_ALLOCATED_INDEX_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param previousAllocatedIdx Last allocated index.
     */
    public void setCandidateAllocatedIndex(long pageAddr, int previousAllocatedIdx) {
        PageUtils.putInt(pageAddr, CANDIDATE_ALLOCATED_INDEX_OFF, previousAllocatedIdx);
    }

    /**
     * @param pageAddr Page address.
     */
    public int getCandidateAllocatedIndex(long pageAddr) {
        return PageUtils.getInt(pageAddr, CANDIDATE_ALLOCATED_INDEX_OFF);
    }

    /**
     * @param pageAddr Page address.
     */
    public long getNextBackupId(long pageAddr) {
        return PageUtils.getLong(pageAddr, NEXT_BACKUP_ID_OFF);
    }
}
