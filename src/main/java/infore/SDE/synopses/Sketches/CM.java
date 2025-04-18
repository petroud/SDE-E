package infore.SDE.synopses.Sketches;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import java.util.Arrays;
import java.util.Random;

import com.clearspring.analytics.stream.membership.Filter;
import com.clearspring.analytics.util.Preconditions;

/**
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 */
public class CM  implements Serializable{

    public static final long PRIME_MODULUS = (1L << 31) - 1;
    private static final long serialVersionUID = -5084982213094657923L;

    int depth;
    int width;


    long[][] table;
    long[] hashA;
    long size;
    double eps;
    double confidence;

    public CM() {
    }

    public CM(int depth, int width, int seed) {
        this.depth = depth;
        this.width = width;
        this.eps = Math.exp(1) / width;
        this.confidence = 1 - 1 / Math.pow(Math.exp(1), depth);
        initTablesWith(depth, width, seed);
    }

    public CM(double epsOfTotalCount, double confidence, int seed) {
        // 2/w = eps ; w = euler/eps
        // 1/2^depth <= 1-confidence ; depth >= -ln (1-confidence)
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(Math.exp(1) / epsOfTotalCount);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(Math.exp(1)));
        initTablesWith(depth, width, seed);
    }

    CM(int depth, int width, long size, long[] hashA, long[][] table) {
        this.depth = depth;
        this.width = width;
        this.eps = Math.exp(1) / width;
        this.confidence = 1 - 1 / Math.pow(Math.exp(1), depth);
        this.hashA = hashA;
        this.table = table;

        Preconditions.checkState(size >= 0, "The size cannot be smaller than ZER0: " + size);
        this.size = size;
    }

    @Override
    public String toString() {
        return "CountMinSketch{" +
                "eps=" + eps +
                ", confidence=" + confidence +
                ", depth=" + depth +
                ", width=" + width +
                ", size=" + size +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CM that = (CM) o;

        if (depth != that.depth) {
            return false;
        }
        if (width != that.width) {
            return false;
        }

        if (Double.compare(that.eps, eps) != 0) {
            return false;
        }
        if (Double.compare(that.confidence, confidence) != 0) {
            return false;
        }

        if (size != that.size) {
            return false;
        }

        if (!Arrays.deepEquals(table, that.table)) {
            return false;
        }
        return Arrays.equals(hashA, that.hashA);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = depth;
        result = 31 * result + width;
        result = 31 * result + Arrays.deepHashCode(table);
        result = 31 * result + Arrays.hashCode(hashA);
        result = 31 * result + (int) (size ^ (size >>> 32));
        temp = Double.doubleToLongBits(eps);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(confidence);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    private void initTablesWith(int depth, int width, int seed) {
        this.table = new long[depth][width];
        this.hashA = new long[depth];
        Random r = new Random(seed);
        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // However we can set b = 0 as all it does is shift the results
        // without compromising their uniformity or independence with
        // the other hashes.
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }

    public double getRelativeError() {
        return eps;
    }

    public double getConfidence() {
        return confidence;
    }

    public long[][] EstimateJoin( CM cm2) {



        return table;
    }


    int hash(long item, int i) {
        long hash = hashA[i] * item;
        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
        return ((int) hash) % width;
    }

    private static void checkSizeAfterOperation(long previousSize, String operation, long newSize) {
        if (newSize < previousSize) {
            throw new IllegalStateException("Overflow error: the size after calling `" + operation +
                    "` is smaller than the previous size. " +
                    "Previous size: " + previousSize +
                    ", New size: " + newSize);
        }
    }

    private void checkSizeAfterAdd(String item, long count) {
        long previousSize = size;
        size += count;
        checkSizeAfterOperation(previousSize, "add(" + item + "," + count + ")", size);
    }

    public void add(long item, long count) {
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        for (int i = 0; i < depth; ++i) {
            table[i][hash(item, i)] += count;
        }

        checkSizeAfterAdd(String.valueOf(item), count);
    }

    public void add(int start, int end, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        long composite_hash = (start * 0x1f1f1f1fL) ^ (end * 0x7f7f7f7fL);
        for (int i = 0; i < depth; ++i) {
            table[i][hash(composite_hash, i)] += count;
        }
        checkSizeAfterAdd(String.valueOf(composite_hash), count);
    }


    public void add(String item, long count) {
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            table[i][buckets[i]] += count;
        }

        checkSizeAfterAdd(item, count);
    }

    public long size() {
        return size;
    }

    /**
     * The estimate is correct within 'epsilon' * (total item count),
     * with probability 'confidence'.
     */
    public long estimateCount(long item) {
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    public long estimateCount(long start, long end) {
        long composite_hash = (start * 0x1f1f1f1fL) ^ (end * 0x7f7f7f7fL);
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(composite_hash, i)]);
        }
        return res;
    }

    public long estimateCount(String item) {
        long res = Long.MAX_VALUE;
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][buckets[i]]);
        }
        return res;
    }


    public void merge(CM  estimator) {
        if (estimator != null ) {
            long size = 0;
                if (this.depth != estimator.depth) {
                    return;
                }
                if (this.width != estimator.width) {
                    return;
                }
                if (!Arrays.equals(this.hashA, estimator.hashA)) {
                    return;
                }

                for (int i = 0; i <this.table.length; i++) {
                    for (int j = 0; j < this.table[i].length; j++) {
                        this.table[i][j] += estimator.table[i][j];
                    }
                }

                long previousSize = size;
                size += estimator.size;
                checkSizeAfterOperation(previousSize, "merge(" + estimator + ")", size);
            }
        }

    public static CM static_merge(CM... estimators) {
        CM merged = null;
        if (estimators != null && estimators.length > 0) {
            int depth = estimators[0].depth;
            int width = estimators[0].width;
            long[] hashA = Arrays.copyOf(estimators[0].hashA, estimators[0].hashA.length);

            long[][] table = new long[depth][width];
            long size = 0;

            for (CM estimator : estimators) {
                if (estimator.depth != depth) {
                   return null;
                }
                if (estimator.width != width) {
                    return null;
                }
                if (!Arrays.equals(estimator.hashA, hashA)) {
                    return null;
                }

                for (int i = 0; i < table.length; i++) {
                    for (int j = 0; j < table[i].length; j++) {
                        table[i][j] += estimator.table[i][j];
                    }
                }

                long previousSize = size;
                size += estimator.size;
                checkSizeAfterOperation(previousSize, "merge(" + estimator + ")", size);
            }

            merged = new CM(depth, width, size, hashA, table);
        }

        return merged;
    }

    public static byte[] serialize(CM sketch) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream s = new DataOutputStream(bos);
        try {
            s.writeLong(sketch.size);
            s.writeInt(sketch.depth);
            s.writeInt(sketch.width);
            for (int i = 0; i < sketch.depth; ++i) {
                s.writeLong(sketch.hashA[i]);
                for (int j = 0; j < sketch.width; ++j) {
                    s.writeLong(sketch.table[i][j]);
                }
            }
            s.close();
            return bos.toByteArray();
        } catch (IOException e) {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }
    public int getDepth() {
        return depth;
    }

    public int getWidth() {
        return width;
    }

    public long[][] getTable() {
        return table;
    }

    public static CM deserialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream s = new DataInputStream(bis);
        try {
            CM sketch = new CM();
            sketch.size = s.readLong();
            sketch.depth = s.readInt();
            sketch.width = s.readInt();
            sketch.eps = Math.exp(1) / sketch.width;
            sketch.confidence = 1 - 1 / Math.pow(Math.exp(1), sketch.depth);
            sketch.hashA = new long[sketch.depth];
            sketch.table = new long[sketch.depth][sketch.width];
            for (int i = 0; i < sketch.depth; ++i) {
                sketch.hashA[i] = s.readLong();
                for (int j = 0; j < sketch.width; ++j) {
                    sketch.table[i][j] = s.readLong();
                }
            }
            return sketch;
        } catch (IOException e) {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }

}

