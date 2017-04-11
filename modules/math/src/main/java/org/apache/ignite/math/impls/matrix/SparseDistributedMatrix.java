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

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.math.*;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.functions.*;
import org.apache.ignite.math.impls.*;
import org.apache.ignite.math.impls.storage.matrix.*;

/**
 * Sparse distributed matrix implementation based on data grid.
 *
 * Unlike {@link CacheMatrix} that is based on existing cache, this implementation creates distributed
 * cache internally and doesn't rely on pre-existing cache.
 *
 * You also need to call {@link #destroy()} to remove the underlying cache when you no longer need this
 * matrix.
 *
 * <b>Currently fold supports only commutative operations.<b/>
 */
public class SparseDistributedMatrix extends AbstractMatrix implements StorageConstants {
    /**
     *
     */
    public SparseDistributedMatrix() {
        // No-op.
    }

    /**
     *
     * @param rows
     * @param cols
     * @param stoMode
     * @param acsMode
     */
    public SparseDistributedMatrix(int rows, int cols, int stoMode, int acsMode) {
        assert rows > 0;
        assert cols > 0;
        assertAccessMode(acsMode);
        assertStorageMode(stoMode);

        setStorage(new SparseDistributedMatrixStorage(rows, cols, stoMode, acsMode));
    }

    /**
     *
     *
     */
    private SparseDistributedMatrixStorage storage() {
        return (SparseDistributedMatrixStorage)getStorage();
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param d
     *
     */
    @Override public Matrix divide(double d) {
        return mapOverValues((Double v) -> v / d);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x
     *
     */
    @Override public Matrix plus(double x) {
        return mapOverValues((Double v) -> v + x);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x
     *
     */
    @Override public Matrix times(double x) {
        return mapOverValues((Double v) -> v * x);
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(double val) {
        return mapOverValues((Double v) -> val);
    }

    /** {@inheritDoc} */
    @Override public Matrix map(IgniteDoubleFunction<Double> fun) {
        mapOverValues(fun::apply);
        storage().setDefaultElement(fun.apply(storage().getDefaultElement()));
        return this;
    }

    /**
     *
     * @param mapper
     *
     */
    private Matrix mapOverValues(IgniteFunction<Double, Double> mapper) {
        // TODO: make sparseMap carry about not storing default values. I.e. if some value was mapped into default, it
        // should be removed.
        CacheUtils.sparseMap(storage().cache().getName(), mapper);

        return this;
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        return CacheUtils.sparseSum(storage().cache().getName(), storage().getDefElsCount());
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        double z = storage().isFull() ? Double.NEGATIVE_INFINITY : storage().getDefaultElement();
        return CacheUtils.sparseFoldNilpotent(storage().cache().getName(),
            (IgniteBiFunction<Map<Integer, Double>, Double, Double>)(map, aDouble) -> Math.max(Collections.max(map.values()), aDouble),
            Math::max, z);
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        double z = storage().isFull() ? Double.POSITIVE_INFINITY : storage().getDefaultElement();
        return CacheUtils.sparseFoldNilpotent(storage().cache().getName(),
            (IgniteBiFunction<Map<Integer, Double>, Double, Double>)(map, aDouble) -> Math.min(Collections.min(map.values()), aDouble),
            Math::min, z);
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }

    /** */
    public Double getDefaultElement() {
        return storage().getDefaultElement();
    }

    /** */
    public long getDefaultElementsCount() {
        return storage().getDefElsCount();
    }
}
