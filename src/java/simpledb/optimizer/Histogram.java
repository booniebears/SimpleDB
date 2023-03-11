package simpledb.optimizer;

import simpledb.execution.Predicate;

public interface Histogram<T> {
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    void addValue(T v);

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
    double estimateSelectivity(Predicate.Op op, T v);

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    double avgSelectivity();

}
