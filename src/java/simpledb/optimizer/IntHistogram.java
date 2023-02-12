package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int ntups;
    private int interval;
    private int[] heights;

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
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        interval = (max - min) / buckets;
        ntups = 0;
        if (buckets > max - min) {
            this.buckets = max - min;
            interval = 1;
        }
        heights = new int[this.buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int i = (v - min) / interval;
        if (v == max) i--;
//        System.out.printf("v:%d , index:%d\n", v, i);
        if (i >= heights.length) {
            System.out.printf("v: %d max: %d min: %d interval: %d\n", v, max, min, interval);
        }
        if (i >= heights.length) i = heights.length - 1;
        heights[i]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     * <p>
     * if f = const, the selectivity of the expression is roughly (h / w) / ntups
     * if f > const,
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
//        v = v < min ? min : v;
//        v = v > max ? max : v;
        int index = (v - min) / interval;
        if (v == max) index--;
        double s = -1.0;
        double sumHeight = 0;

        if (v < min || v > max) {
            switch (op) {
                case EQUALS:
                    return 0;
                case NOT_EQUALS:
                    return 1;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return v < min ? 1 : 0;
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return v < min ? 0 : 1;
            }
        }

        switch (op) {
            case EQUALS:
                s = heights[index] * 1.0 / (ntups * interval);
                break;
            case NOT_EQUALS:
                s = 1 - heights[index] * 1.0 / (ntups * interval);
                break;
            case GREATER_THAN:
                for (int i = index + 1; i < heights.length; i++) {
                    sumHeight += heights[i];
                }
                s = sumHeight / ntups;
                break;
            case GREATER_THAN_OR_EQ:
                int right = min + (index + 1) * interval;
                sumHeight = heights[index] * (right - v) / interval;
                for (int i = index + 1; i < heights.length; i++) {
                    sumHeight += heights[i];
                }
                s = sumHeight / ntups;
                break;
            case LESS_THAN:
                for (int i = index - 1; i >= 0; i--) {
                    sumHeight += heights[i];
                }
                s = sumHeight / ntups;
                break;
            case LESS_THAN_OR_EQ:
                int left = min + index * interval;
                sumHeight = heights[index] * (v - left) / interval;
                if (v == left && interval == 1)
                    sumHeight = heights[index] * 0.5;
                for (int i = index - 1; i >= 0; i--) {
                    sumHeight += heights[i];
                }
                s = sumHeight / ntups;
                break;
        }
        return s;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        int sum = 0;
        for (int i = 0; i < heights.length; i++) {
            sum += heights[i];
        }
        return sum / ntups;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" + "buckets=" + buckets + ", min=" + min + ", max=" + max + ", ntups=" + ntups + ", interval=" + interval + ", heights=" + Arrays.toString(heights) + '}';
    }
}
