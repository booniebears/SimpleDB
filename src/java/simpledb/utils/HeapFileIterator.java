package simpledb.utils;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

    private final TransactionId tid;
    private final HeapFile heapFile;

    private int pgNo;
    private Iterator<Tuple> tupleIterator;

    public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
        this.tid = tid;
        this.heapFile = heapFile;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        //open the iterator, so the pgNo starts from 0, and tupleIterator is created;
        //Since we have implemented iterator for tuples on one page in HeapPage.java,
        //we use it here in HeapFileIterator.
        pgNo = 0;
        tupleIterator = getTupleIterator(pgNo);
    }

    public Iterator<Tuple> getTupleIterator(int pgNo) throws TransactionAbortedException, DbException {
        // We get the "Page" specified by "pgNo"
        if (pgNo >= 0 && pgNo < heapFile.numPages()) {
            HeapPageId heapPageId = new HeapPageId(heapFile.getId(), pgNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId,
                    Permissions.READ_ONLY);
            return page.iterator();
        } else {
            System.out.println("heapFile length = " + heapFile.getFile().length());
            System.out.println("NumOfPages = " + heapFile.numPages());
            throw new DbException(String.format("heapFile %d does not exist in page[%d]!",
                    heapFile.getId(), pgNo));
        }
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        //Iterator not open
        if (tupleIterator == null) return false;
        while (!tupleIterator.hasNext()) {
            pgNo++;
            //Notice that a page may not hold any valid tuple. So "tupleIterator.hasNext()" is checked.
            if (pgNo < heapFile.numPages()) {
                tupleIterator = getTupleIterator(pgNo);
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return tupleIterator.next();
        }
        throw new NoSuchElementException("No more tuples on this HeapFile");
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // rewind = close + open
        close();
        open();
    }

    @Override
    public void close() {
        tupleIterator = null;
    }
}
