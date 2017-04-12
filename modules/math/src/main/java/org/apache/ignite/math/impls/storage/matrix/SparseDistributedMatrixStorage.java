// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.math.impls.storage.matrix;

import it.unimi.dsi.fastutil.ints.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.impls.*;
import java.io.*;
import java.util.*;

/**
 * TODO: add description.
 */
public class SparseDistributedMatrixStorage extends CacheUtils implements MatrixStorage, StorageConstants {
    private int rows, cols;
    private int stoMode, acsMode;
    private long defElsCount;
    private Double defEl;

    /** Actual distributed storage. */
    private IgniteCache<
        Integer /* Row or column index. */,
        Map<Integer, Double> /* Map-based row or column. */
    > cache = null;

    /**
     *
     */
    public SparseDistributedMatrixStorage() {
        // No-op.
    }

    /**
     *
     * @param rows
     * @param cols
     * @param stoMode
     * @param acsMode
     */
    public SparseDistributedMatrixStorage(int rows, int cols, int stoMode, int acsMode) {
        assert rows > 0;
        assert cols > 0;
        assertAccessMode(acsMode);
        assertStorageMode(stoMode);

        this.rows = rows;
        this.cols = cols;
        this.stoMode = stoMode;
        this.acsMode = acsMode;
        defElsCount = rows * cols;
        defEl = 0.0;

        cache = newCache();
    }

    /**
     *
     *
     */
    private IgniteCache<Integer, Map<Integer, Double>> newCache() {
        CacheConfiguration<Integer, Map<Integer, Double>> cfg = new CacheConfiguration<>();

        // Assume 10% density.
        cfg.setStartSize(Math.max(1024, (rows * cols) / 10));

        // Write to primary.
        cfg.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        // Atomic transactions only.
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        // No eviction.
        cfg.setEvictionPolicy(null);

        // No copying of values.
        cfg.setCopyOnRead(false);

        // Cache is partitioned.
        cfg.setCacheMode(CacheMode.PARTITIONED);

        // Random cache name.
        //TODO: check if this fix is ok. This fix we needed because new IgniteUuid().shortString() always gave same string
//        cfg.setName(new IgniteUuid().shortString());
        cfg.setName(UUID.randomUUID().toString());

        return Ignition.localIgnite().getOrCreateCache(cfg);
    }

    /**
     * 
     *
     */
    public IgniteCache<Integer, Map<Integer, Double>> cache() {
        return cache;
    }

    /**
     *
     *
     */
    public int accessMode() {
        return acsMode;
    }

    /**
     * 
     *
     */
    public int storageMode() {
        return stoMode;
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        if (stoMode == ROW_STORAGE_MODE)
            return matrixGet(cache.getName(), x, y);
        else
            return matrixGet(cache.getName(), y, x);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        if (stoMode == ROW_STORAGE_MODE)
            matrixSet(cache.getName(), x, y, v);
        else
            matrixSet(cache.getName(), y, x, v);
    }

    /**
     * Distributed matrix get.
     *
     * @param cacheName Matrix's cache.
     * @param a Row or column index.
     * @param b Row or column index.
     * @return Matrix value at (a, b) index.
     */
    private double matrixGet(String cacheName, int a, int b) {
        // Remote get from the primary node (where given row or column is stored locally).
        return ignite().compute(groupForKey(cacheName, a)).call(() -> {
            IgniteCache<Integer, Map<Integer, Double>> cache = Ignition.localIgnite().getOrCreateCache(cacheName);

            // Local get.
            Map<Integer, Double> map = cache.localPeek(a, CachePeekMode.PRIMARY);

            return (map == null || !map.containsKey(b)) ? defEl : map.get(b);
        });
    }

    /**
     * Distributed matrix set.
     *
     * @param cacheName Matrix's cache.
     * @param a Row or column index.
     * @param b Row or column index.
     * @param v New value to set.
     */
    private void matrixSet(String cacheName, int a, int b, double v) {
        // Remote set on the primary node (where given row or column is stored locally).
        defElsCount += ignite().compute(groupForKey(cacheName, a)).call(() -> {
            int increment = 0;

            IgniteCache<Integer, Map<Integer, Double>> cache = Ignition.localIgnite().getOrCreateCache(cacheName);

            // Local get.
            Map<Integer, Double> map = cache.localPeek(a, CachePeekMode.PRIMARY);

            if (map == null)
                map = acsMode == SEQUENTIAL_ACCESS_MODE ? new Int2DoubleRBTreeMap() : new Int2DoubleOpenHashMap();

            if (v != defEl) {
                Double prev = map.put(b, v);
                if (prev == null)
                    increment = -1;
            }
            else if (map.containsKey(b)) {
                map.remove(b);
                increment = 1;
            }

            // Local put.
            cache.put(a, map);
            return increment;
        });
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);
        out.writeInt(acsMode);
        out.writeInt(stoMode);
        out.writeUTF(cache.getName());
        out.writeDouble(defEl);
        out.writeLong(defElsCount);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        acsMode = in.readInt();
        stoMode = in.readInt();
        cache = ignite().getOrCreateCache(in.readUTF());
        defEl = in.readDouble();
        defElsCount = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return acsMode == SEQUENTIAL_ACCESS_MODE;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return acsMode == RANDOM_ACCESS_MODE;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false; 
    }

    /** Destroy underlying cache. */
    @Override public void destroy() {
        cache.destroy();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + cols;
        res = res * 37 + rows;
        res = res * 37 + acsMode;
        res = res * 37 + stoMode;
        res = res * 37 + cache.hashCode();
        res = (int)(res * 37 + defEl);
        res = (int)(res * 37 + defElsCount);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        SparseDistributedMatrixStorage that = (SparseDistributedMatrixStorage) obj;

        return rows == that.rows && cols == that.cols && acsMode == that.acsMode && stoMode == that.stoMode
            && (cache != null ? cache.equals(that.cache) : that.cache == null)
            && (defEl != null ? defEl.equals(that.defEl) : that.defEl== null)
            && (defElsCount == that.defElsCount);
    }

    /** */
    public boolean isFull() {
        return defElsCount == 0;
    }

    /** */
    public Double getDefaultElement() {
        return defEl;
    }

    /** */
    public void setDefaultElement(Double defEl) {
        this.defEl = defEl;
        long nonDefEls = 0;
        String cacheName = cache().getName();

        for (int i = 0; i < rows; i++) {
            final int y = i;
            nonDefEls += ignite().compute(groupForKey(cacheName, i)).call(() -> {
                IgniteCache<Integer, Map<Integer, Double>> cache = Ignition.localIgnite().getOrCreateCache(cacheName);

                // Local get.
                Map<Integer, Double> map = cache.localPeek(y, CachePeekMode.PRIMARY);

                if (map == null)
                    return 0;

                // Count number of non-default elements
                int res = 0;
                Iterator<Map.Entry<Integer, Double>> iter = map.entrySet().iterator();

                while (iter.hasNext()) {
                    Map.Entry<Integer, Double> next = iter.next();
                    if (next.getValue().equals(defEl)) {
                        iter.remove();
                        res += 1;
                    }
                }

                return res;
            });
        }

        defElsCount += nonDefEls;
    }

    /** */
    public long getDefElsCount() {
        return defElsCount;
    }
}
