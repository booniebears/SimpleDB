package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final int tableId;
    private final int ioCostPerPage;
    private final int numPages;
    private int numTuples;

    /**
     * I need to establish the relationship between every column in a table and the relevant
     * histogram. Therefore a map is introduced, and to generalize a new generic interface called
     * "Histogram<T>" is introduced, with class IntHistogram and StringHistogram as its
     * implementations. The map is used in the method "estimateSelectivity"
     */
    private final Map<Integer, Histogram> histogramMap;
    private final TupleDesc td;


    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.numTuples = 0;
        HeapFile dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.td = dbFile.getTupleDesc();
        this.numPages = dbFile.numPages();
        this.histogramMap = new HashMap<>();
        // Build histogram for every field
        // We need the minVal, maxVal of a column to build a histogram, therefore we have to scan
        // all the tuples first, which is implemented by "fetchFieldValues()".
        Map<Integer, ArrayList> fieldMap = fetchFieldValues();
        for (int fieldId : fieldMap.keySet()) {
            if (td.getFieldType(fieldId) == Type.INT_TYPE) {
                List<Integer> list = fieldMap.get(fieldId);
                int min = Collections.min(list);
                int max = Collections.max(list);
                Histogram<Integer> integerHistogram = new IntHistogram(NUM_HIST_BINS, min, max);
                for (Integer v : list) {
                    integerHistogram.addValue(v);
                }
                histogramMap.put(fieldId, integerHistogram);
            } else if (td.getFieldType(fieldId) == Type.STRING_TYPE) {
                List<String> list = fieldMap.get(fieldId);
                Histogram<String> stringHistogram = new StringHistogram(NUM_HIST_BINS);
                for (String v : list) {
                    stringHistogram.addValue(v);
                }
                histogramMap.put(fieldId, stringHistogram);
            }
        }
    }

    private Map<Integer, ArrayList> fetchFieldValues() {
        Map<Integer, ArrayList> fieldMap = new HashMap<>();
        // Note that a new TransactionId is created.
        SeqScan seqScan = new SeqScan(new TransactionId(), tableId);
        // Allocate space for ArrayList
        for (int i = 0; i < td.numFields(); i++) {
            // Only two types are concerned in SimpleDb
            if (td.getFieldType(i) == Type.INT_TYPE) {
                fieldMap.put(i, new ArrayList<Integer>());
            } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                fieldMap.put(i, new ArrayList<String>());
            }
        }
        try {
            seqScan.open();
            while (seqScan.hasNext()) {
                numTuples++;
                Tuple next = seqScan.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        IntField field = (IntField) (next.getField(i));
                        fieldMap.get(i).add(field.getValue());
                    } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                        StringField field = (StringField) next.getField(i);
                        if (!field.getValue().equals("")) {
                            fieldMap.get(i).add(field.getValue());
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fieldMap;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        // Cardinality means "基数", which is the number of different keys in a column.
        return (int) (selectivityFactor * numTuples);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        switch (td.getFieldType(field)) {
            case INT_TYPE: {
                IntHistogram histogram = (IntHistogram) histogramMap.get(field);
                return histogram.avgSelectivity();
            }
            case STRING_TYPE: {
                StringHistogram histogram = (StringHistogram) histogramMap.get(field);
                return histogram.avgSelectivity();
            }
        }
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (histogramMap.containsKey(field)) {
            switch (td.getFieldType(field)) {
                case INT_TYPE: {
                    IntHistogram histogram = (IntHistogram) histogramMap.get(field);
                    return histogram.estimateSelectivity(op, ((IntField) constant).getValue());
                }
                case STRING_TYPE: {
                    StringHistogram histogram = (StringHistogram) histogramMap.get(field);
                    return histogram.estimateSelectivity(op, ((StringField) constant).getValue());
                }
            }
        }
        return 0.0;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return numTuples;
    }

}
