package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int groupByField;
    Type groupByFieldType;
    int aggField;
    Op op;
    // groupBy field as the key
    Map<Field, List<StringField>> map;
    List<Tuple> tuples;
    TupleDesc tupleDesc;

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
        groupByField = gbfield;
        groupByFieldType = gbfieldtype;
        aggField = afield;
        op = what;
        map = new HashMap<>();
        tupleDesc = new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field f = tup.getField(groupByField);
        if (!map.containsKey(f))
            map.put(f, new ArrayList<>());
        map.get(f).add((StringField) tup.getField(aggField));
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        if (tuples != null)
            return new TupleIterator(tuples.get(0).getTupleDesc(), tuples);
        // foreach the group and calculate the aggregate value of each group
        tuples = new ArrayList<>();
        for (Field f : map.keySet()) {
            IntField res = calculate(map.get(f));
            Tuple t = new Tuple(tupleDesc);
            t.setField(0, f);
            t.setField(1, res);
            tuples.add(t);
        }
        return iterator();
    }

    public IntField calculate(List<StringField> fields) {
        if (fields == null || fields.size() == 0)
            return null;
        int v;
        switch (op) {
            case COUNT:
                v = fields.size();
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return new IntField(v);
    }

}
