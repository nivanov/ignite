// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

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

package org.apache.ignite.math.impls.matrix;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.StorageConstants;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.impls.MathTestConstants;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.math.impls.MathTestConstants.STORAGE_SIZE;
import static org.apache.ignite.math.impls.MathTestConstants.UNEXPECTED_VALUE;

/**
 * Tests for {@ling SparseDistributedMatrix}.
 */
@GridCommonTest(group = "Distributed Models")
public class SparseDistributedMatrixTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;
    /** Cache name. */
    private static final String CACHE_NAME = "test-cache";
    public static final double PRESITION = 0.0;
    /** Grid instance. */
    private Ignite ignite;
    /** Matrix rows */
    private final int rows = MathTestConstants.STORAGE_SIZE;
    /** Matrix cols */
    private final int cols = MathTestConstants.STORAGE_SIZE;
    /** Matrix for tests */
    private SparseDistributedMatrix cacheMatrix;

    /**
     * Default constructor.
     */
    public SparseDistributedMatrixTest(){
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *  {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);

        ignite.configuration().setPeerClassLoadingEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.destroyCache(CACHE_NAME);

        if (cacheMatrix != null){
            cacheMatrix.destroy();
            cacheMatrix = null;
        }
    }

    /** */
    public void testGetSet() throws Exception {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double v = Math.random();
                cacheMatrix.set(i, j, v);

                assert Double.compare(v, cacheMatrix.get(i, j)) == 0;
            }
        }
    }

    /** */
    public void testExternalize() throws IOException, ClassNotFoundException {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        cacheMatrix.set(1, 1, 1.0);

        ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objOutputStream = new ObjectOutputStream(byteArrOutputStream);

        objOutputStream.writeObject(cacheMatrix);

        ByteArrayInputStream byteArrInputStream = new ByteArrayInputStream(byteArrOutputStream.toByteArray());
        ObjectInputStream objInputStream = new ObjectInputStream(byteArrInputStream);

        SparseDistributedMatrix objRestored = (SparseDistributedMatrix) objInputStream.readObject();

        assertTrue(MathTestConstants.VALUE_NOT_EQUALS, cacheMatrix.equals(objRestored));
        assertEquals(MathTestConstants.VALUE_NOT_EQUALS, objRestored.get(1, 1), 1.0, 0.0);
    }

    /** Test simple math. */
    public void testMath(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
        initMtx(cacheMatrix);

        cacheMatrix.assign(2.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VALUE, 2.0, cacheMatrix.get(i, j), PRESITION);

        cacheMatrix.plus(3.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VALUE, 5.0, cacheMatrix.get(i, j), PRESITION);

        cacheMatrix.times(2.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VALUE, 10.0, cacheMatrix.get(i, j), PRESITION);

        cacheMatrix.divide(10.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VALUE, 1.0, cacheMatrix.get(i, j), PRESITION);

        assertEquals(UNEXPECTED_VALUE, cacheMatrix.rowSize() * cacheMatrix.columnSize(), cacheMatrix.sum(), PRESITION);
    }

    /** */
    public void testMinMax(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                cacheMatrix.set(i, j, i * cols + j + 1);

        assertEquals(UNEXPECTED_VALUE, 1.0, cacheMatrix.minValue(), PRESITION);
        assertEquals(UNEXPECTED_VALUE, rows * cols, cacheMatrix.maxValue(), PRESITION);

        cacheMatrix.assign(0.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                cacheMatrix.set(i, j, -1.0 * (i * cols + j + 1));

        assertEquals(UNEXPECTED_VALUE, - rows * cols, cacheMatrix.minValue(), PRESITION);
        assertEquals(UNEXPECTED_VALUE, - 1.0, cacheMatrix.maxValue(), PRESITION);

        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                cacheMatrix.set(i, j, i * cols + j);

        assertEquals(UNEXPECTED_VALUE, 0.0, cacheMatrix.minValue(), PRESITION);
        assertEquals(UNEXPECTED_VALUE, rows * cols - 1.0, cacheMatrix.maxValue(), PRESITION);

        // Non-full matrix
        for (int i = 2; i < cacheMatrix.rowSize(); i += 2)
            for (int j = 2; j < cacheMatrix.columnSize(); j += 2)
                cacheMatrix.set(i, j, i * cols + j + 1);

        assertEquals(UNEXPECTED_VALUE, 0.0, cacheMatrix.minValue(), PRESITION);

        // Non-full matrix
        for (int i = 2; i < cacheMatrix.rowSize(); i += 2)
            for (int j = 2; j < cacheMatrix.columnSize(); j += 2)
                cacheMatrix.set(i, j, i * cols + j + 1);
    }

    /**
     * Tests the 'map' function when operand matrix is full with values
     */
    public void testMapFull() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
        initMtx(cacheMatrix);

        cacheMatrix.map(i -> 100.0);
        for (int i = 0; i < cacheMatrix.rowSize(); i++)
            for (int j = 0; j < cacheMatrix.columnSize(); j++)
                assertEquals(UNEXPECTED_VALUE, 100.0, cacheMatrix.get(i, j), PRESITION);
    }

    /** */
    public void testMapSparse() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        // Set only even indexes.
        setEven(cacheMatrix, (integer, integer2) -> 33.0);

        assertEquals("3/4 of elements should have default value", 3 * rows * cols / 4, cacheMatrix.getDefaultElementsCount());
        cacheMatrix.map(i -> 100.0);

        // get on odd index should also return mapped value;
        assertEquals(UNEXPECTED_VALUE, 100, cacheMatrix.get(63, 79), PRESITION);
        assertEquals("All elements now should have default value", rows * cols, cacheMatrix.getDefaultElementsCount());
    }

    /** */
    public void testCopy(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        try {
            cacheMatrix.copy();
            fail("UnsupportedOperationException expected.");
        } catch (UnsupportedOperationException e){
            return;
        }
        fail("UnsupportedOperationException expected.");
    }

    /** */
    public void testLike(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        try {
            cacheMatrix.like(1, 1);
            fail("UnsupportedOperationException expected.");
        } catch (UnsupportedOperationException e){
            return;
        }
        fail("UnsupportedOperationException expected.");
    }

    /** */
    public void testLikeVector(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        cacheMatrix = new SparseDistributedMatrix(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        try {
            cacheMatrix.likeVector(1);
            fail("UnsupportedOperationException expected.");
        } catch (UnsupportedOperationException e){
            return;
        }
        fail("UnsupportedOperationException expected.");
    }

    /** */
    private void initMtx(Matrix m){
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, 1.0);
    }

    private void setEven(Matrix m, BiFunction<Integer, Integer, Double> f) {
        for (int i = 0; i < rows; i+=2)
            for (int j = 0; j < cols; j+=2)
                m.set(i, j, f.apply(i, j));

    }
}
