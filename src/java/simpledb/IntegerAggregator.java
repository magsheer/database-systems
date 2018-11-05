package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final TupleDesc Td;
    private Map<Field, Integer> aggregateData;
    private Map<Field, Integer> count;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregateData = new HashMap<>();
        this.count = new HashMap<>();
        String[] fields;
        Type[] types;
        if (gbfield == -1) {
            fields = new String[]{"aggregate values"};
            types = new Type[]{Type.INT_TYPE};
        } else {
            fields = new String[]{"group values", "aggregate values"};
            types = new Type[]{this.gbfieldtype, Type.INT_TYPE};
        }
        Td = new TupleDesc(types, fields);
    }

    private int loadMap() {
        switch (what) {
            case MIN:
                return Integer.MAX_VALUE;
            case MAX:
                return Integer.MIN_VALUE;
            case SUM:
            case COUNT:
            case AVG:
                return 0;
        }
        return 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField = (gbfield == -1) ? null : tup.getField(gbfield);

        if (!aggregateData.containsKey(groupByField)) {
            aggregateData.put(groupByField, loadMap());
            count.put(groupByField, 0);
        }

        int value = ((IntField) tup.getField(afield)).getValue();
        int curr = aggregateData.get(groupByField);
        int currCount = count.get(groupByField);
        int next = curr;
        switch (what) {
            case MIN:
                next = Math.min(curr, value);
                break;
            case MAX:
                next = Math.max(curr, value);
                break;
            case SUM:
            case AVG:
                count.put(groupByField, currCount + 1);
                next = value + curr;
                break;
            case COUNT:
                next = curr + 1;
                break;
        }
        aggregateData.put(groupByField, next);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Field field : aggregateData.keySet()) {
            int aggregate;
            if (what == Op.AVG) {
                aggregate = aggregateData.get(field) / count.get(field);
            } else {
                aggregate = aggregateData.get(field);
            }
            Tuple tup = new Tuple(Td);
            if (gbfield == -1) {
                tup.setField(0, new IntField(aggregate));
            } else {
                tup.setField(0, field);
                tup.setField(1, new IntField(aggregate));
            }
            tuples.add(tup);
        }
        return new TupleIterator(Td, tuples);
    }
}
