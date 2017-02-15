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

package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.VectorStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstract class with base tests for each vector storage.
 */
public abstract class VectorBaseStorageTest<T extends VectorStorage> extends ExternalizeTest<T> {
    /** */
    protected T storage;

    /** */
    @Before
    public abstract void setUp();

    /** */
    @After
    public void tearDown() throws Exception {
        storage.destroy();
    }

    /** */
    @Test
    public void getSet() throws Exception {
        for (int i = 0; i < STORAGE_SIZE; i++) {
            double random = Math.random();

            storage.set(i, random);

            assertEquals(WRONG_DATA_ELEMENT, storage.get(i), random, NIL_DELTA);
        }
    }

    /** */
    @Test
    public void size(){
        assertTrue(UNEXPECTED_VALUE, storage.size() == STORAGE_SIZE);
    }

    /** */
    @Override public void externalizeTest() {
        super.externalizeTest(storage);
    }

    /**
     * Fill storage by random doubles.
     *
     * @param size Storage size.
     */
    private void fillStorage(int size) {
        for (int i = 0; i < size; i++)
            storage.set(i, Math.random());
    }
}
