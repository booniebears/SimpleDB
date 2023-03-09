package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;

    private Map<Field, AggInfo> groupMap;
    private TupleDesc td;
    private Field DEFAULT_FIELD = new StringField("Default", 10);

    /**
     * The method mergeTupleIntoGroup() processes only one tuple at a time, so we have to record the
     * aggregateVal whenever the method is called if we want to process all the tuples. Therefore,
     * the private class "AggInfo" is introduced here. It works for all the Aggregators required,
     * like (COUNT, SUM, AVG, MIN, MAX).
     */
    private static class AggInfo {
        private int cnt = 0;
        private int sum = 0;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
    }

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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //Process one tuple at a time.
        if (td == null) {
            //Build TupleDesc for td
            buildTupleDesc(tup.getTupleDesc());
        }
        Field groupField = tup.getField(gbField);
        IntField aggField = (IntField) tup.getField(aField);
        if (gbField == NO_GROUPING) {
            Aggregating(DEFAULT_FIELD, aggField.getValue());
        } else {
            Aggregating(groupField, aggField.getValue());
        }
    }

    /**
     * The actual Aggregation procedure. The results are temporarily stored in "groupMap".
     */
    private void Aggregating(Field key, int value) {
        if (key != null) {
            //getOrDefault ensures that aggInfo is not null
            AggInfo aggInfo = groupMap.getOrDefault(key, new AggInfo());
            switch (op) {
                case MIN: {
                    aggInfo.min = Math.min(value, aggInfo.min);
                    break;
                }
                case MAX: {
                    aggInfo.max = Math.max(value, aggInfo.max);
                    break;
                }
                case SUM: {
                    aggInfo.sum += value;
                    break;
                }
                case AVG: {
                    aggInfo.sum += value;
                    aggInfo.cnt++;
                    break;
                }
                case COUNT: {
                    aggInfo.cnt++;
                    break;
                }
            }
            groupMap.put(key, aggInfo);
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
     * Return the correct Aggregation value based on the groupMap
     */
    private int parseValue(Field key) {
        if (key != null && groupMap.containsKey(key)) {
            AggInfo aggInfo = groupMap.get(key);
            switch (op) {
                case MIN: {
                    return aggInfo.min;
                }
                case MAX: {
                    return aggInfo.max;
                }
                case COUNT: {
                    return aggInfo.cnt;
                }
                case SUM: {
                    return aggInfo.sum;
                }
                case AVG: {
                    return aggInfo.sum / aggInfo.cnt;
                }
            }
        }
        return 0;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tupleList = new ArrayList<>();
        if (gbField == NO_GROUPING) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(parseValue(DEFAULT_FIELD)));
            tupleList.add(tuple);
        } else {
            groupMap.forEach((key, info) -> {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, key); //key是Field类型,group by分组用到的值
                tuple.setField(1, new IntField(parseValue(key)));
                tupleList.add(tuple);
            });
        }
        return new TupleIterator(td, tupleList);
    }
}
