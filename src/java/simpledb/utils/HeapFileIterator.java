package simpledb.utils;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator extends AbstractDbFileIterator {

    private final TransactionId tid;
    private final HeapFile heapFile;
    private HeapPage heapPage;
    private int pgNo;
    private Iterator<Tuple> tupleIterator = null;

    public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
        this.tid = tid;
        this.heapFile = heapFile;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        //Since we have implemented iterator for tuples on one page in HeapPage.java,
        //we use it here in HeapFileIterator.
        HeapPageId pageId = new HeapPageId(heapFile.getId(), pgNo);
        heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        tupleIterator = heapPage.iterator();
    }

//    public Iterator<Tuple> getTupleIterator(int pgNo) throws TransactionAbortedException, DbException {
//        // We get the "Page" specified by "pgNo"
//        if (pgNo >= 0 && pgNo < heapFile.numPages()) {
//            HeapPageId heapPageId = new HeapPageId(heapFile.getId(), pgNo);
//            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId,
//                    Permissions.READ_ONLY);
//            return page.iterator();
//        } else {
//            System.out.println("heapFile length = " + heapFile.getFile().length());
//            System.out.println("NumOfPages = " + heapFile.numPages());
//            throw new DbException(String.format("heapFile %d does not exist in page[%d]!",
//                    heapFile.getId(), pgNo));
//        }
//    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (tupleIterator == null)
            return null;
        while (!tupleIterator.hasNext()) { //If no more tuples on the given page, look for the next one
            pgNo++;
            if (pgNo < heapFile.numPages()) {
                HeapPageId pageId = new HeapPageId(heapFile.getId(), pgNo);
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                tupleIterator = heapPage.iterator();
            } else {
                return null;
            }
        }
        return tupleIterator.next();
    }

//    @Override
//    public boolean hasNext() throws DbException, TransactionAbortedException {
//        //Iterator not open
//        if (tupleIterator == null) return false;
//        while (!tupleIterator.hasNext()) {
//            pgNo++;
//            //Notice that a page may not hold any valid tuple. So "tupleIterator.hasNext()" is checked.
//            if (pgNo < heapFile.numPages()) {
//                tupleIterator = getTupleIterator(pgNo);
//            } else {
//                return false;
//            }
//        }
//        return true;
//    }


    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // rewind = close + open
        close();
        open();
    }

    @Override
    public void close() {
        super.close();
        tupleIterator = null;
        pgNo = 0;
    }

}
