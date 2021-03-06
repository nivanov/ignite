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

package org.apache.ignite.math.impls.vector;

import java.util.Map;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.VectorStorage;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.math.impls.storage.vector.ArrayVectorStorage;

/**
 * Basic implementation for vector.
 * <p>
 * This is a trivial implementation for vector assuming dense logic, local on-heap JVM storage
 * based on {@code double[]} array. It is only suitable for data sets where
 * local, non-distributed execution is satisfactory and on-heap JVM storage is enough
 * to keep the entire data set.
 */
public class DenseLocalOnHeapVector extends AbstractVector {
    /**
     * @param size Vector cardinality.
     */
    private VectorStorage mkStorage(int size) {
        return new ArrayVectorStorage(size);
    }

    /**
     * @param arr Source array.
     * @param cp {@code true} to clone array, reuse it otherwise.
     */
    private VectorStorage mkStorage(double[] arr, boolean cp) {
        assert arr != null;

        return new ArrayVectorStorage(cp ? arr.clone() : arr);
    }

    /**
     * @param args Parameters for new Vector.
     */
    public DenseLocalOnHeapVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size"))
            setStorage(mkStorage((int)args.get("size")));
        else if (args.containsKey("arr") && args.containsKey("copy"))
            setStorage(mkStorage((double[])args.get("arr"), (boolean)args.get("copy")));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /** */
    public DenseLocalOnHeapVector() {
        // No-op.
    }

    /**
     * @param size Vector cardinality.
     */
    public DenseLocalOnHeapVector(int size) {
        setStorage(mkStorage(size));
    }

    /**
     * @param arr Source array.
     * @param shallowCp {@code true} to use shallow copy.
     */
    public DenseLocalOnHeapVector(double[] arr, boolean shallowCp) {
        setStorage(mkStorage(arr, shallowCp));
    }

    /**
     * @param arr Source array.
     */
    public DenseLocalOnHeapVector(double[] arr) {
        this(arr, false);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return new DenseLocalOnHeapMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new DenseLocalOnHeapVector(crd);
    }
}
