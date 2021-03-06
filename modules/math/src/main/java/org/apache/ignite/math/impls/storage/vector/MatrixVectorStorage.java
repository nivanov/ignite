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

package org.apache.ignite.math.impls.storage.vector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.VectorStorage;
import org.apache.ignite.math.exceptions.IndexException;

/**
 * Row, column or diagonal vector-based view of the matrix
 */
public class MatrixVectorStorage implements VectorStorage {
    /** */ private Matrix parent;

    /** */ private int row;
    /** */ private int col;

    /** */ private int rowStride;
    /** */  private int colStride;

    /** */ private int size;

    /**
     *
     */
    public MatrixVectorStorage() {
        // No-op.
    }

    /**
     * @param parent
     * @param row
     * @param col
     * @param rowStride
     * @param colStride
     */
    public MatrixVectorStorage(Matrix parent, int row, int col, int rowStride, int colStride) {
        assert parent != null;
        assert rowStride >= 0;
        assert colStride >= 0;
        assert rowStride > 0 || colStride > 0;

        if (row < 0 || row >= parent.rowSize())
            throw new IndexException(row);
        if (col < 0 || col >= parent.columnSize())
            throw new IndexException(col);

        this.parent = parent;

        this.row = row;
        this.col = col;

        this.rowStride = rowStride;
        this.colStride = colStride;

        this.size = getSize();
    }

    /**
     *
     *
     */
    int row() {
        return row;
    }

    /**
     *
     *
     */
    int column() {
        return col;
    }

    /**
     *
     *
     */
    int rowStride() {
        return rowStride;
    }

    /**
     *
     *
     */
    int columnStride() {
        return colStride;
    }

    /**
     *
     *
     */
    private int getSize() {
        if (rowStride != 0 && colStride != 0) {
            int n1 = (parent.rowSize() - row) / rowStride;
            int n2 = (parent.columnSize() - col) / colStride;

            return Math.min(n1, n2);
        }
        else if (rowStride > 0)
            return (parent.rowSize() - row) / rowStride;
        else
            return (parent.columnSize() - col) / colStride;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return parent.get(row + i * rowStride, col + i * colStride);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        parent.set(row + i * rowStride, col + i * colStride, v);
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return parent.isSequentialAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return parent.isDense();
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return parent.isRandomAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return parent.isDistributed();
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(parent);
        out.writeInt(row);
        out.writeInt(col);
        out.writeInt(rowStride);
        out.writeInt(colStride);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        parent = (Matrix)in.readObject();
        row = in.readInt();
        col = in.readInt();
        rowStride = in.readInt();
        colStride = in.readInt();

        size = getSize();
    }
}
