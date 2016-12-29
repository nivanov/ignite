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

import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;

/**
 * We use dedicated page for tracking pages updates.
 * Also we divide such 'tracking' pages on two half, first is used for check that page was changed or not
 * (during incremental snapshot), second - to accumulate changed for next snapshot.
 *
 * You cannot test change for not started snapshot! because it will cause of preparation for snapshot.
 *
 * Implementation. For each page there is own bit in both half. Tracking page is used for tracking N page after it.
 * N depends on page size (how many bytes we can use for tracking).
 *
 *
 *                      +-----------------------------------------+-----------------------------------------+
 *                      |                left half                |               right half                |
 * +---------+----------+----+------------------------------------+----+------------------------------------+
 * |  HEADER | Last     |size|                                    |size|                                    |
 * |         |SnapshotId|2b. |  tracking bits                     |2b. |  tracking bits                     |
 * +---------+----------+----+------------------------------------+----+------------------------------------+
 *
 */
public class TrackingPageIO extends PageIO {
    /** */
    public static final IOVersions<TrackingPageIO> VERSIONS = new IOVersions<>(
        new TrackingPageIO(1)
    );

    /** Last snapshot offset. */
    public static final int LAST_SNAPSHOT_TAG_OFFSET = COMMON_HEADER_END;

    /** Size field offset. */
    public static final int SIZE_FIELD_OFFSET = LAST_SNAPSHOT_TAG_OFFSET + 8;

    /** 'Size' field size. */
    public static final int SIZE_FIELD_SIZE = 2;

    /** Bitmap offset. */
    public static final int BITMAP_OFFSET = SIZE_FIELD_OFFSET + SIZE_FIELD_SIZE;

    /** Count of extra page. */
    public static final int COUNT_OF_EXTRA_PAGE = 1;

    /**
     * @param ver Page format version.
     */
    protected TrackingPageIO(int ver) {
        super(PageIO.T_PAGE_UPDATE_TRACKING, ver);
    }

    /**
     * Will mark pageId as changed for next (!) snapshotId
     *
     * @param pageAddr Page address.
     * @param pageId Page id.
     * @param nextSnapshotTag tag of next snapshot.
     * @param pageSize Page size.
     */
    public boolean markChanged(long pageAddr, long pageId, long nextSnapshotTag, long lastSuccessfulSnapshotTag, int pageSize) {
        validateSnapshotId(pageAddr, nextSnapshotTag, lastSuccessfulSnapshotTag, pageSize);

        int cntOfPage = countOfPageToTrack(pageSize);

        int idxToUpdate = (PageIdUtils.pageIndex(pageId) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        int sizeOff = useLeftHalf(nextSnapshotTag) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + (cntOfPage >> 3);

        int idx = sizeOff + SIZE_FIELD_SIZE + (idxToUpdate >> 3);

        byte byteToUpdate = PageUtils.getByte(pageAddr, idx);

        int updateTemplate = 1 << (idxToUpdate & 0b111);

        byte newVal =  (byte) (byteToUpdate | updateTemplate);

        if (byteToUpdate == newVal)
            return false;

        PageUtils.putByte(pageAddr, idx, newVal);

        short newSize = (short)(PageUtils.getShort(pageAddr, sizeOff) + 1);

        PageUtils.putShort(pageAddr, sizeOff, newSize);

        assert newSize == countOfChangedPage(pageAddr, nextSnapshotTag, pageSize);

        return true;
    }

    /**
     * @param pageAddr Page address.
     * @param nextSnapshotTag Next snapshot id.
     * @param lastSuccessfulSnapshotId Last successful snapshot id.
     * @param pageSize Page size.
     */
    private void validateSnapshotId(long pageAddr, long nextSnapshotTag, long lastSuccessfulSnapshotId, int pageSize) {
        assert nextSnapshotTag != lastSuccessfulSnapshotId : "nextSnapshotTag = " + nextSnapshotTag +
            ", lastSuccessfulSnapshotId = " + lastSuccessfulSnapshotId;

        long last = getLastSnapshotTag(pageAddr);

        assert last <= nextSnapshotTag : "last = " + last + ", nextSnapshotTag = " + nextSnapshotTag;

        if (nextSnapshotTag == last) //everything is ok
            return;

        int cntOfPage = countOfPageToTrack(pageSize);

        if (last <= lastSuccessfulSnapshotId) { //we can drop our data
            PageUtils.putLong(pageAddr, LAST_SNAPSHOT_TAG_OFFSET, nextSnapshotTag);

            PageHandler.zeroMemory(pageAddr, SIZE_FIELD_OFFSET, pageSize - SIZE_FIELD_OFFSET);
        } else { //we can't drop data, it is still necessary for incremental snapshots
            int len = cntOfPage >> 3;

            int sizeOff = useLeftHalf(nextSnapshotTag) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + len;
            int sizeOff2 = !useLeftHalf(nextSnapshotTag) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + len;

            if (last - lastSuccessfulSnapshotId == 1) { //we should keep only data in last half
                //new data will be written in the same half, we should move old data to another half
                if ((nextSnapshotTag - last) % 2 == 0)
                    PageHandler.copyMemory(pageAddr, pageAddr, sizeOff, sizeOff2, len + SIZE_FIELD_SIZE);
            } else { //last - lastSuccessfulSnapshotId > 1, e.g. we should merge two half in one
                int newSize = 0;
                int i = 0;

                for (; i < len - 8; i += 8) {
                    long newVal = PageUtils.getLong(pageAddr, sizeOff + SIZE_FIELD_SIZE + i) | PageUtils.getLong(pageAddr, sizeOff2 + SIZE_FIELD_SIZE + i);

                    newSize += Long.bitCount(newVal);

                    PageUtils.putLong(pageAddr, sizeOff2 + SIZE_FIELD_SIZE + i, newVal);
                }

                for (; i < len; i ++) {
                    byte newVal = (byte)(PageUtils.getByte(pageAddr, sizeOff + SIZE_FIELD_SIZE + i) | PageUtils.getByte(pageAddr, sizeOff2 + SIZE_FIELD_SIZE + i));

                    newSize += Integer.bitCount(newVal & 0xFF);

                    PageUtils.putByte(pageAddr, sizeOff2 + SIZE_FIELD_SIZE + i, newVal);
                }

                PageUtils.putShort(pageAddr, sizeOff2, (short)newSize);
            }

            PageUtils.putLong(pageAddr, LAST_SNAPSHOT_TAG_OFFSET, nextSnapshotTag);

            PageHandler.zeroMemory(pageAddr, sizeOff, len + SIZE_FIELD_SIZE);
        }
    }

    /**
     * @param pageAddr Page address.
     */
    long getLastSnapshotTag(long pageAddr) {
        return PageUtils.getLong(pageAddr, LAST_SNAPSHOT_TAG_OFFSET);
    }

    /**
     * Check that pageId was marked as changed between previous snapshot finish and current snapshot start.
     *
     * @param pageAddr Page address.
     * @param pageId Page id.
     * @param curSnapshotTag Snapshot tag.
     * @param pageSize Page size.
     */
    public boolean wasChanged(long pageAddr, long pageId, long curSnapshotTag, long lastSuccessfulSnapshotTag, int pageSize) {
        validateSnapshotId(pageAddr, curSnapshotTag + 1, lastSuccessfulSnapshotTag, pageSize);

        if (countOfChangedPage(pageAddr, curSnapshotTag, pageSize) < 1)
            return false;

        int cntOfPage = countOfPageToTrack(pageSize);

        int idxToTest = (PageIdUtils.pageIndex(pageId) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        byte byteToTest;

        if (useLeftHalf(curSnapshotTag))
            byteToTest = PageUtils.getByte(pageAddr, BITMAP_OFFSET + (idxToTest >> 3));
        else
            byteToTest = PageUtils.getByte(pageAddr, BITMAP_OFFSET + SIZE_FIELD_SIZE + ((idxToTest + cntOfPage) >> 3));

        int testTemplate = 1 << (idxToTest & 0b111);

        return ((byteToTest & testTemplate) ^ testTemplate) == 0;
    }

    /**
     * @param pageAddr Page address.
     * @param snapshotTag Snapshot tag.
     * @param pageSize Page size.
     *
     * @return count of pages which were marked as change for given snapshotTag
     */
    public short countOfChangedPage(long pageAddr, long snapshotTag, int pageSize) {
        long dif = getLastSnapshotTag(pageAddr) - snapshotTag;

        if (dif != 0 && dif != 1)
            return -1;

        if (useLeftHalf(snapshotTag))
            return PageUtils.getShort(pageAddr, SIZE_FIELD_OFFSET);
        else
            return PageUtils.getShort(pageAddr, BITMAP_OFFSET + (countOfPageToTrack(pageSize) >> 3));
    }

    /**
     * @param snapshotTag Snapshot id.
     *
     * @return true if snapshotTag is odd, otherwise - false
     */
    boolean useLeftHalf(long snapshotTag) {
        return (snapshotTag & 0b1) == 0;
    }

    /**
     * @param pageId Page id.
     * @param pageSize Page size.
     * @return pageId of tracking page which set pageId belongs to
     */
    public long trackingPageFor(long pageId, int pageSize) {
        assert PageIdUtils.pageIndex(pageId) > 0;

        int pageIdx = ((PageIdUtils.pageIndex(pageId) - COUNT_OF_EXTRA_PAGE) /
            countOfPageToTrack(pageSize)) * countOfPageToTrack(pageSize) + COUNT_OF_EXTRA_PAGE;

        long trackingPageId = PageIdUtils.pageId(PageIdUtils.partId(pageId), PageIdUtils.flag(pageId), pageIdx);

        assert PageIdUtils.pageIndex(trackingPageId) <= PageIdUtils.pageIndex(pageId);

        return trackingPageId;
    }

    /**
     * @param pageSize Page size.
     *
     * @return how many page we can track with 1 page
     */
    public int countOfPageToTrack(int pageSize) {
        return ((pageSize - SIZE_FIELD_OFFSET) / 2 - SIZE_FIELD_SIZE)  << 3;
    }

    /**
     * @param pageAddr Page address.
     * @param start Start.
     * @param curSnapshotTag Snapshot id.
     * @param pageSize Page size.
     * @return set pageId if it was changed or next closest one, if there is no changed page null will be returned
     */
    public Long findNextChangedPage(long pageAddr, long start, long curSnapshotTag, long lastSuccessfulSnapshotTag, int pageSize) {
        validateSnapshotId(pageAddr, curSnapshotTag + 1, lastSuccessfulSnapshotTag, pageSize);

        int cntOfPage = countOfPageToTrack(pageSize);

        long trackingPage = trackingPageFor(start, pageSize);

        if (start == trackingPage)
            return trackingPage;

        if (countOfChangedPage(pageAddr, curSnapshotTag, pageSize) <= 0)
            return null;

        int idxToStartTest = (PageIdUtils.pageIndex(start) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        int zeroIdx = useLeftHalf(curSnapshotTag)? BITMAP_OFFSET : BITMAP_OFFSET + SIZE_FIELD_SIZE + (cntOfPage >> 3);

        int startIdx = zeroIdx + (idxToStartTest >> 3);

        int idx = startIdx;

        int stopIdx = zeroIdx + (cntOfPage >> 3);

        while (idx < stopIdx) {
            byte byteToTest = PageUtils.getByte(pageAddr, idx);

            if (byteToTest != 0) {
                int foundSetBit;
                if ((foundSetBit = foundSetBit(byteToTest, idx == startIdx ? (idxToStartTest & 0b111) : 0)) != -1) {
                    long foundPageId = PageIdUtils.pageId(
                        PageIdUtils.partId(start),
                        PageIdUtils.flag(start),
                        PageIdUtils.pageIndex(trackingPage) + ((idx - zeroIdx) << 3) + foundSetBit);

                    assert wasChanged(pageAddr, foundPageId, curSnapshotTag, lastSuccessfulSnapshotTag, pageSize);
                    assert trackingPageFor(foundPageId, pageSize) == trackingPage;

                    return foundPageId;
                }
            }

            idx++;
        }

        return null;
    }

    /**
     * @param byteToTest Byte to test.
     * @param firstBitToTest First bit to test.
     */
    private static int foundSetBit(byte byteToTest, int firstBitToTest) {
        assert firstBitToTest < 8;

        for (int i = firstBitToTest; i < 8; i++) {
            int testTemplate = 1 << i;

            if (((byteToTest & testTemplate) ^ testTemplate) == 0)
                return i;
        }

        return -1;
    }
}
