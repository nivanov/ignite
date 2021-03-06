package org.apache.ignite.math.impls.vector;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.impls.matrix.DenseLocalOffHeapMatrix;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.math.impls.matrix.RandomMatrix;
import org.apache.ignite.math.impls.matrix.SparseLocalOnHeapMatrix;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for methods of Vector that involve Matrix. */
public class VectorToMatrixTest {
    /** */
    private static final Map<Class<? extends Vector>, Class<? extends Matrix>> typesMap = typesMap();

    /** */
    private static final List<Class<? extends Vector>> likeMatrixUnsupported = Arrays.asList(FunctionVector.class,
        SingleElementVector.class, SingleElementVectorView.class, ConstantVector.class);

    /** */
    @Test
    public void testHaveLikeMatrix() throws InstantiationException, IllegalAccessException {
        for (Class<? extends Vector> key : typesMap.keySet()) {
            Class<? extends Matrix> val = typesMap.get(key);

            if (val == null && likeMatrixSupported(key))
                System.out.println("Missing test for implementation of likeMatrix for " + key.getSimpleName());
        }
    }

    /** */
    @Test
    public void testLikeMatrixUnsupported() throws Exception {
        consumeSampleVectors((v, desc) -> {
            if (likeMatrixSupported(v.getClass()))
                return;

            boolean expECaught = false;

            try {
                assertNull("Null view instead of exception in " + desc, v.likeMatrix(1, 1));
            }
            catch (UnsupportedOperationException uoe) {
                expECaught = true;
            }

            assertTrue("Expected exception was not caught in " + desc, expECaught);
        });
    }

    /** */
    @Test
    public void testLikeMatrix() {
        consumeSampleVectors((v, desc) -> {
            if (!availableForTesting(v))
                return;

            final Matrix matrix = v.likeMatrix(1, 1);

            Class<? extends Vector> key = v.getClass();

            Class<? extends Matrix> expMatrixType = typesMap.get(key);

            assertNotNull("Expect non-null matrix for " + key.getSimpleName() + " in " + desc, matrix);

            Class<? extends Matrix> actualMatrixType = matrix.getClass();

            assertTrue("Expected matrix type " + expMatrixType.getSimpleName()
                    + " should be assignable from actual type " + actualMatrixType.getSimpleName() + " in " + desc,
                expMatrixType.isAssignableFrom(actualMatrixType));

            for (int rows : new int[] {1, 2})
                for (int cols : new int[] {1, 2}) {
                    final Matrix actualMatrix = v.likeMatrix(rows, cols);

                    String details = "rows " + rows + " cols " + cols;

                    assertNotNull("Expect non-null matrix for " + details + " in " + desc,
                        actualMatrix);

                    assertEquals("Unexpected number of rows in " + desc, rows, actualMatrix.rowSize());

                    assertEquals("Unexpected number of cols in " + desc, cols, actualMatrix.columnSize());
                }
        });
    }

    /** */
    @Test
    public void testToMatrix() {
        consumeSampleVectors((v, desc) -> {
            if (!availableForTesting(v))
                return;

            fillWithNonZeroes(v);

            final Matrix matrixRow = v.toMatrix(true);

            final Matrix matrixCol = v.toMatrix(false);

            for (Vector.Element e : v.all())
                assertToMatrixValue(desc, matrixRow, matrixCol, e.get(), e.index());
        });
    }

    /** */
    @Test
    public void testToMatrixPlusOne() {
        consumeSampleVectors((v, desc) -> {
            if (!availableForTesting(v))
                return;

            fillWithNonZeroes(v);

            for (double zeroVal : new double[] {-1, 0, 1, 2}) {
                final Matrix matrixRow = v.toMatrixPlusOne(true, zeroVal);

                final Matrix matrixCol = v.toMatrixPlusOne(false, zeroVal);

                final Metric metricRow0 = new Metric(zeroVal, matrixRow.get(0, 0));

                assertTrue("Not close enough row like " + metricRow0 + " at index 0 in " + desc,
                    metricRow0.closeEnough());

                final Metric metricCol0 = new Metric(zeroVal, matrixCol.get(0, 0));

                assertTrue("Not close enough cols like " + metricCol0 + " at index 0 in " + desc,
                    metricCol0.closeEnough());

                for (Vector.Element e : v.all())
                    assertToMatrixValue(desc, matrixRow, matrixCol, e.get(), e.index() + 1);
            }
        });
    }

    /** */
    @Test
    public void testCross() {
        consumeSampleVectors((v, desc) -> {
            if (!availableForTesting(v))
                return;

            fillWithNonZeroes(v);

            for (int delta : new int[] {-1, 0, 1}) {
                final int size2 = v.size() + delta;

                if (size2 < 1)
                    return;

                final Vector v2 = new DenseLocalOnHeapVector(size2);

                for (Vector.Element e : v2.all())
                    e.set(size2 - e.index());

                assertCross(v, v2, desc);
            }
        });
    }

    /** */
    private void assertCross(Vector v1, Vector v2, String desc) {
        assertNotNull(v1);
        assertNotNull(v2);

        final Matrix res = v1.cross(v2);

        assertNotNull("Cross matrix is expected to be not null in " + desc, res);

        assertEquals("Unexpected number of rows in cross Matrix in " + desc, v1.size(), res.rowSize());

        assertEquals("Unexpected number of cols in cross Matrix in " + desc, v2.size(), res.columnSize());

        for (int row = 0; row < v1.size(); row++)
            for (int col = 0; col < v2.size(); col++) {
                final Metric metric = new Metric(v1.get(row) * v2.get(col), res.get(row, col));

                assertTrue("Not close enough cross " + metric + " at row " + row + " at col " + col
                    + " in " + desc, metric.closeEnough());
            }
    }

    /** */
    private void assertToMatrixValue(String desc, Matrix matrixRow, Matrix matrixCol, double exp, int idx) {
        final Metric metricRow = new Metric(exp, matrixRow.get(0, idx));

        assertTrue("Not close enough row like " + metricRow + " at index " + idx + " in " + desc,
            metricRow.closeEnough());

        final Metric metricCol = new Metric(exp, matrixCol.get(idx, 0));

        assertTrue("Not close enough cols like " + matrixCol + " at index " + idx + " in " + desc,
            metricCol.closeEnough());
    }

    /** */
    private void fillWithNonZeroes(Vector sample) {
        if (sample instanceof RandomVector)
            return;

        for (Vector.Element e : sample.all())
            e.set(1 + e.index());
    }

    /** */
    private boolean availableForTesting(Vector v) {
        assertNotNull("Error in test: vector is null", v);

        if (!likeMatrixSupported(v.getClass()))
            return false;

        final boolean availableForTesting = typesMap.get(v.getClass()) != null;

        final Matrix actualLikeMatrix = v.likeMatrix(1, 1);

        assertTrue("Need to enable matrix testing for vector type " + v.getClass().getSimpleName(),
            availableForTesting || actualLikeMatrix == null);

        return availableForTesting;
    }

    /** Ignore test for given vector type. */
    private boolean likeMatrixSupported(Class<? extends Vector> clazz) {
        for (Class<? extends Vector> ignoredClass : likeMatrixUnsupported)
            if (ignoredClass.isAssignableFrom(clazz))
                return false;

        return true;
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Vector, String> consumer) {
        new VectorImplementationsFixtures().consumeSampleVectors(null, consumer);
    }

    /** */
    private static Map<Class<? extends Vector>, Class<? extends Matrix>> typesMap() {
        return new LinkedHashMap<Class<? extends Vector>, Class<? extends Matrix>>() {{
            put(DenseLocalOnHeapVector.class, DenseLocalOnHeapMatrix.class);
            put(DenseLocalOffHeapVector.class, DenseLocalOffHeapMatrix.class);
            put(RandomVector.class, RandomMatrix.class);
            put(SparseLocalVector.class, SparseLocalOnHeapMatrix.class);
            put(SingleElementVector.class, null); // todo find out if we need SingleElementMatrix to match, or skip it
            put(ConstantVector.class, null);
            put(FunctionVector.class, null);
            put(PivotedVectorView.class, DenseLocalOnHeapMatrix.class); // IMPL NOTE per fixture
            put(SingleElementVectorView.class, null);
            put(MatrixVectorView.class, DenseLocalOnHeapMatrix.class); // IMPL NOTE per fixture
            put(DelegatingVector.class, DenseLocalOnHeapMatrix.class); // IMPL NOTE per fixture
            // IMPL NOTE check for presence of all implementations here will be done in testHaveLikeMatrix via Fixture
        }};
    }

    /** */
    private static class Metric { // todo consider if softer tolerance (like say 0.1 or 0.01) would make sense here
        /** */
        private final double exp;

        /** */
        private final double obtained;

        /** **/
        Metric(double exp, double obtained) {
            this.exp = exp;
            this.obtained = obtained;
        }

        /** */
        boolean closeEnough() {
            return new Double(exp).equals(obtained);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Metric{" + "expected=" + exp +
                ", obtained=" + obtained +
                '}';
        }
    }
}
