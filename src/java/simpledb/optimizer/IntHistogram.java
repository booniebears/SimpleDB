package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram<Integer>{
    // Histogram means "直方图"

    private int width; // width指的是除了最后一个bucket之外,每个bucket的横坐标宽度
    private int lastWidth; // 最后一个bucket的横坐标宽度. Note that it may be greater than width.
    private int min;
    private int max;
    private int[] buckets;
    private int numTuples;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.numTuples = 0;
        this.width = Math.max(1, (max - min + 1) / buckets);
        this.lastWidth = max - min + 1 - ((buckets - 1) * width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(Integer v) {
        // some code goes here
        if (v >= min && v <= max) {
            int index = (v - min) / width;
            if (index < buckets.length) {
                buckets[index]++;
                numTuples++;
            }
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, Integer v) {
        // some code goes here
        switch (op) {
            case GREATER_THAN: {
                if (v >= max) return 0;
                else if (v < min) return 1.0;
                //Suppose that the values are distributed uniformly in each bucket
                int index = (v - min) / width;
                int len = buckets.length;
                int tuplesCnt = buckets[index] * (min + (index + 1) * width - v - 1) / width;
                if (index == len - 1) {
                    tuplesCnt = buckets[index] * (max - v) / lastWidth;
                }
                for (int i = index + 1; i < len; i++) {
                    tuplesCnt += buckets[i];
                }
                return 1.0 * tuplesCnt / numTuples;
            }
            case LESS_THAN_OR_EQ: {
                return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            }
            case GREATER_THAN_OR_EQ: {
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            }
            case LESS_THAN: {
                return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
            }
            case EQUALS: {
                return estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v) -
                        estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            }
            case NOT_EQUALS: {
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            default:
                return -1.0;
        }
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        double res = 0;
        for (int bucket : buckets) {
            res += 1.0 * bucket / numTuples;
        }
        return res / buckets.length;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        // some code goes here
        return "IntHistogram{" +
                "width=" + width +
                ", lastWidth=" + lastWidth +
                ", min=" + min +
                ", max=" + max +
                ", buckets=" + Arrays.toString(buckets) +
                ", numTuples=" + numTuples +
                '}';
    }
}
