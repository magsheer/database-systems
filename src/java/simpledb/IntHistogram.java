package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int buckets;
    private final int min;
    private final int mod;
    private int count;
    private int[] bucketArray;

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
        this.buckets = buckets;
        this.min = min;
        this.bucketArray = new int[buckets];
        this.mod = (int) Math.ceil((double) (max - min + 1) / buckets);
        this.count = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        bucketArray[(v - min) / mod]++;
        count++;
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
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        switch (op) {
            case EQUALS:
            case LIKE:
                return estimateSelectivityForEqualsLike(Predicate.Op.EQUALS, v);
            case NOT_EQUALS:
                return 1.0 - estimateSelectivityForEqualsLike(Predicate.Op.EQUALS, v);
            case GREATER_THAN:
            case LESS_THAN:
                return estimateSelectivityForComparison(op, v);
            case GREATER_THAN_OR_EQ:
                return (estimateSelectivityForEqualsLike(Predicate.Op.EQUALS, v) + estimateSelectivityForComparison(Predicate.Op.GREATER_THAN, v));
            case LESS_THAN_OR_EQ:
                return (estimateSelectivityForEqualsLike(Predicate.Op.EQUALS, v) + estimateSelectivityForComparison(Predicate.Op.LESS_THAN, v));
            default:
                return -1.0;
        }
    }

    private int intializeBucket(int bucket) {
        if (bucket < 0)
            bucket = -1;
        if (bucket >= buckets)
            bucket = buckets;
        return bucket;
    }

    private double estimateSelectivityForEqualsLike(Predicate.Op op, int v) {
        int bucket = (v - min) / mod;
        bucket = intializeBucket(bucket);
        if (bucket < 0)
            return 0.0;
        if (bucket >= buckets)
            return 0.0;
        int height = bucketArray[bucket];
        return ((double) height / mod) / count;
    }

    private double estimateSelectivityForComparison(Predicate.Op op, int v) {
        int bucket = (v - min) / mod;
        bucket = intializeBucket(bucket);
        int curr = 0;
        int right;
        int left;
        int h = 0;

        if (bucket < 0) {
            right = 0;
            left = -1;
        } else if (bucket >= buckets) {
            right = buckets;
            left = buckets - 1;
        } else {
            right = bucket + 1;
            left = bucket - 1;
            curr = -1;
            h = bucketArray[bucket];
        }
        double value = (float) (h * curr) / count;
        switch (op) {
            case LESS_THAN:
                if (left < 0)
                    return value / count;
                for (int i = left; i >= 0; i--) {
                    value = value + bucketArray[i];
                }
                return value / count;
            case GREATER_THAN:
                if (right >= buckets)
                    return value / count;
                for (int i = right; i < buckets; i++) {
                    value = value + bucketArray[i];
                }
                return value / count;
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
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
