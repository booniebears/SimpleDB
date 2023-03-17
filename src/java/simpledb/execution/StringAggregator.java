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
    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Op op;

    /**
     * Since the StringAggregator only supports "COUNT" here, we don't have to introduce another
     * AggInfo like that in IntegerAggregator.
     */
    private final Map<Field, Integer> groupMap;
    private TupleDesc td;
    private final Field DEFAULT_FIELD = new StringField("Default", 10);

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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (td == null) {
            buildTupleDesc(tup.getTupleDesc());
        }
        Field groupField = tup.getField(gbField);
        Field target = (gbField == NO_GROUPING) ? DEFAULT_FIELD : groupField;
        if (op == Op.COUNT) {
            groupMap.put(target, groupMap.getOrDefault(target, 0) + 1);
        }
    }

    private void buildTupleDesc(TupleDesc originalTd) {
        if (this.gbField == NO_GROUPING) {
            Type[] types = new Type[]{Type.INT_TYPE};
            String[] strings = new String[]{"aggregateVal"};
            td = new TupleDesc(types, strings);
        } else {
            Type[] types = new Type[]{gbFieldType, Type.INT_TYPE};
            String[] strings = new String[]{originalTd.getFieldName(gbField),
                    originalTd.getFieldName(aField)};
            td = new TupleDesc(types, strings);
        }
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
        // some code goes here
        List<Tuple> tupleList = new ArrayList<>();
        if (gbField == NO_GROUPING) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(groupMap.get(DEFAULT_FIELD)));
            tupleList.add(tuple);
        } else {
            groupMap.forEach((key, cnt) -> {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(cnt));
                tupleList.add(tuple);
            });
        }
        return new TupleIterator(td, tupleList);
    }

}
