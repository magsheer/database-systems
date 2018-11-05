package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private Map<Field, Integer> count;
    private TupleDesc Td;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
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

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField = (gbfield == -1) ? null : tup.getField(gbfield);

        if (!count.containsKey(groupByField)) {
            count.put(groupByField, 0);
        }

        int currCount = count.get(groupByField);
        count.put(groupByField, currCount + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Field field : count.keySet()) {
            int aggregate = count.get(field);
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
