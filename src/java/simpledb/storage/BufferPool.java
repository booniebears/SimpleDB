package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.LockType;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.utils.LRUCache;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    //    private LRUCache lruCache; // A mapping from PageId to Page.
    private final LockManager lockManager;
    private final Map<PageId, Page> pageMap;
    private final int numPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
//        this.lruCache = new LRUCache(numPages);
        this.lockManager = new LockManager();
        this.pageMap = new HashMap<>();
        this.numPages = numPages;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        long start = System.currentTimeMillis();
        long timeOut = (long) (200 + Math.random() * 1000L);
        while (!this.lockManager.tryLock(tid, pid, perm)) {
            if (System.currentTimeMillis() - start > timeOut)
                throw new TransactionAbortedException();
        }
        if (pageMap.containsKey(pid)) return pageMap.get(pid);
        //if the page is not in the BufferPool, then load the page into it.
        return LoadNewPage(pid);
    }

    private Page LoadNewPage(PageId pid) throws DbException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbFile.readPage(pid);
//        if (page != null) {
//            if (lruCache.getMaxSize() == lruCache.getSize())
//                evictPage();
//            lruCache.put(pid, page);
//        }
        if (pageMap.size() >= numPages) {
            synchronized (this.pageMap) {
                evictPage();
            }
        }
        pageMap.putIfAbsent(pid, page);
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLockOnPage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> collections = lockManager.getPageIds(tid);
        synchronized (this.pageMap) {
            if (commit) {
                collections.forEach(pageId -> {
                    if (pageMap.containsKey(pageId) && Objects.equals(pageMap.get(pageId).isDirty(), tid)) {
                        try {
                            flushPage(pageId);
                            pageMap.get(pageId).setBeforeImage();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            } else {
                Set<PageId> exLockPages = lockManager.getEXLockPages(tid);
                collections.forEach(pageId -> {
                    if (pageMap.containsKey(pageId) && exLockPages.contains(pageId)) {
                        pageMap.remove(pageId);
                    }
                });
            }
        }
        lockManager.releaseAllLocks(tid);
        //        if (commit) { //Commit successfully
//            try {
//                flushPages(tid);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        } else {
//            restorePages(tid);
//        }
        // Releasing any locks that the transaction held
    }


    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        for (Page page : pages) {
//            lruCache.put(page.getId(), page);
            page.markDirty(true, tid);
            pageMap.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // "tableId" can be ascertained by the information of "Tuple t"
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for (Page page : pages) {
//            lruCache.put(page.getId(), page);
            pageMap.put(page.getId(), page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
//        Iterator<Page> iterator = lruCache.valueIterator();
//        // Iterate through all the pages in LRUCache, and flush all of them to disk
//        while (iterator.hasNext()) {
//            PageId pid = iterator.next().getId();
//            System.out.println("Flushing All pages!");
//            flushPage(pid);
//        }
        pageMap.forEach((pageId, page) -> {
            try {
                flushPage(pageId);
                page.setBeforeImage();
                page.markDirty(false, null);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        // Implement this in lab2!!!!!!!!!
//        lruCache.remove(pid);
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//        Page page = lruCache.get(pid);
        Page page = pageMap.get(pid);
        // TODO: logfile???
        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // Go through all the pages in BufferPool
//        Iterator<Page> pageIterator = lruCache.valueIterator();
//        while (pageIterator.hasNext()) {
//            Page next = pageIterator.next();
//            if (next.isDirty() == tid) {
//                flushPage(next.getId());
//            }
//        }
        pageMap.forEach((pageId, page) -> {
            if (page.isDirty() == tid) {
                try {
                    flushPage(pageId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // Evict the pages that are Least Recently Used first. Note that we choose to evict those pages
        // that are not dirty, so that a flush operation is not needed.
//        Iterator<Page> iterator = lruCache.reverseIterator();
//        while (iterator.hasNext()) {
//            Page next = iterator.next();
//            if (next.isDirty() == null) {
//                discardPage(next.getId());
//                return;
//            }
//        }
//        throw new DbException("All the pages are dirty in BufferPool!");
        if (pageMap.size() < numPages) { // pageMap未满,不作处理
            return;
        }
        boolean evicted = false;
        Iterator<Map.Entry<PageId, Page>> iterator = pageMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> next = iterator.next();
            if (next.getValue().isDirty() == null) { // Evict a page that is not dirty
                iterator.remove();
                evicted = true;
            }
        }
        if (!evicted)
            throw new DbException("evict Fail");
    }
}

