package simpledb.optimizer;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    int tupleNum;
    int tableId;
    int pageNum;
    int ioCostPerPage;
    Type[] typeArr;
    int[] maxArr;
    int[] minArr;
    Map<Integer, IntHistogram> intHistogramMap;
    Map<Integer, StringHistogram> stringHistogramMap;

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
        // TODO: some code goes here
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        pageNum = heapFile.numPages();
        tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        TupleDesc tupleDesc = heapFile.getTupleDesc();
        int length = tupleDesc.numFields();
        typeArr = new Type[length];
        minArr = new int[length];
        maxArr = new int[length];
        intHistogramMap = new HashMap<>();
        stringHistogramMap = new HashMap<>();
        tupleNum = 0;
        for (int i = 0; i < length; i++) {
            minArr[i] = Integer.MAX_VALUE;
            maxArr[i] = Integer.MIN_VALUE;
            typeArr[i] = tupleDesc.getFieldType(i);
        }
        DbFileIterator iterator = heapFile.iterator(null);

        try {
            iterator.open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        IntField intField;
        StringField stringField;

        while (true) {
            try {
                if (!iterator.hasNext()) break;
                tupleNum++;
                Tuple t = iterator.next();
                List<Field> fields = t.getAllFields();
                int i = 0;
                int v;
                for (Field f : fields) {
                    switch (f.getType()) {
                        case INT_TYPE:
                            intField = (IntField) f;
                            v = intField.getValue();
                            if (v > maxArr[i]) maxArr[i] = v;
                            if (v < minArr[i]) minArr[i] = v;
                            break;
                        case STRING_TYPE:
                            stringField = (StringField) f;
                            v = stringToInt(stringField.getValue());
                            if (v > maxArr[i]) maxArr[i] = v;
                            if (v < minArr[i]) minArr[i] = v;
                    }
                    i++;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        //TODO how to decide the buckets?
        for (int i = 0; i < length; i++) {
            switch (typeArr[i]) {
                case INT_TYPE:
                    IntHistogram intHistogram = new IntHistogram(maxArr[i] - minArr[i], minArr[i], maxArr[i]);
                    intHistogramMap.put(i, intHistogram);
                    break;
                case STRING_TYPE:
                    StringHistogram stringHistogram = new StringHistogram(maxArr[i] - minArr[i]);
                    stringHistogramMap.put(i, stringHistogram);
                    break;
            }
        }

        try {
            iterator.rewind();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        while (true) {
            try {
                IntHistogram intHistogram;
                StringHistogram stringHistogram;
                if (!iterator.hasNext()) break;
                Tuple t = iterator.next();
                List<Field> fields = t.getAllFields();
                int i = 0;
                int v;
                for (Field f : fields) {
                    switch (f.getType()) {
                        case INT_TYPE:
                            intField = (IntField) f;
                            intHistogramMap.get(i).addValue(intField.getValue());
                            break;
                        case STRING_TYPE:
                            stringField = (StringField) f;
                            stringHistogramMap.get(i).addValue(stringField.getValue());
                            break;
                    }
                    i++;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


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
        return pageNum * ioCostPerPage;
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
        return (int) (tupleNum * selectivityFactor);
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
        Type type = typeArr[field];
        switch (type) {
            case STRING_TYPE:
                StringHistogram stringHistogram = stringHistogramMap.get(field);
                return stringHistogram.avgSelectivity();
            case INT_TYPE:
                IntHistogram intHistogram = intHistogramMap.get(field);
                return intHistogram.avgSelectivity();
        }
        return -1;
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
        Type type = typeArr[field];
        switch (type) {
            case STRING_TYPE:
                StringField stringField = (StringField) constant;
                StringHistogram stringHistogram = stringHistogramMap.get(field);
                return stringHistogram.estimateSelectivity(op, stringField.getValue());
            case INT_TYPE:
                IntField intField = (IntField) constant;
                IntHistogram intHistogram = intHistogramMap.get(field);
                return intHistogram.estimateSelectivity(op, intField.getValue());
        }
        return -1;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return tupleNum;
    }

    private int stringToInt(String s) {
        int i;
        int v = 0;
        for (i = 3; i >= 0; i--) {
            if (s.length() > 3 - i) {
                int ci = s.charAt(3 - i);
                v += (ci) << (i * 8);
            }
        }
        // XXX: hack to avoid getting wrong results for
        // strings which don't output in the range min to max
        if (!(s.equals("") || s.equals("zzzz"))) {
            if (v < 0) {
                v = 0;
            }
            if (v > 2054847098) {
                v = 2054847098;
            }
        }
        return v;
    }

}
