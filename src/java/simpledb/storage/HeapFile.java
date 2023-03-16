package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.utils.HeapFileIterator;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private RandomAccessFile randomAccessFile;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
//        System.out.println("HeapFile Initialization!");
//        System.out.println("File path = " + f);
        this.f = f;
        this.td = td;
        try {
            this.randomAccessFile = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // Here we calculate the "offset" of the given page specified by pid. Then by using
        // randomAccessFile, we can read the correct page from the file.
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        try {
            randomAccessFile.seek(offset);
            byte[] buffer = new byte[BufferPool.getPageSize()];
            if (randomAccessFile.read(buffer) != -1) {
                return new HeapPage((HeapPageId) pid, buffer);
            } else {
                HeapPage page = new HeapPage((HeapPageId) pid, buffer);
                this.writePage(page);
                return page;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int offset = BufferPool.getPageSize() * page.getId().getPageNumber();
        randomAccessFile.seek(offset);
        byte[] pageData = page.getPageData();
        randomAccessFile.write(pageData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    // TODO: Question: Why the variable returned is of "List<Page>" type? If we insert/delete a tuple,
    // only one page will be changed.
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // To insert a tuple into a HeapFile, we first iterate through all the pages the heapFile
        // contains; Then we call the insertTuple() of class "HeapPage" if there are empty slots
        // remaining; Also remember to call markDirty() to store the TransactionId.
        List<Page> modifyList = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i),
                    Permissions.READ_WRITE);
            if (page != null && page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                modifyList.add(page);
                break;
            }
        }
        if (modifyList.size() == 0) {
            // This indicates that all pages remaining are full. Therefore, a new page should be created.
            HeapPageId heapPageId = new HeapPageId(getId(), numPages());
            // Fantastic! The class HeapPage provides the method createEmptyPageData().
            HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            writePage(heapPage);
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            heapPage.insertTuple(t);
            heapPage.markDirty(true, tid);
            modifyList.add(heapPage);
        }
        return modifyList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pageList = new ArrayList<>();
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(),
                Permissions.READ_WRITE);
        if (page != null && page.isSlotUsed(recordId.getTupleNumber())) {
            page.deleteTuple(t);
            pageList.add(page);
        }
        return pageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
//        System.out.println("HeapFile length = " + f.length());
        return new HeapFileIterator(tid, this);
    }

}

