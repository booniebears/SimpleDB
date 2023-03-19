package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Record the lock information on each page. The information includes the pageId, TransactionId and
 * the LockType, which can be implemented with a ConcurrentHashMap.
 */
public class LockManager {
    //  locking at page granularity(粒度)
//    private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, LockType>> lockMap;
//    private static final int X_LOCK_WAIT_TIME = 100;
//    private static final int S_LOCK_WAIT_TIME = 100;
//    private static final int MAX_RETRY = 3;

    private final Map<TransactionId, Set<PageId>> tpMap;
    private final Map<PageId, PageLock> ppMap;

    /*Have to make thorough changes.*/
    public LockManager() {
//        lockMap = new ConcurrentHashMap<>();
        tpMap = new ConcurrentHashMap<>();
        ppMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean tryLock(TransactionId tid, PageId pid, Permissions perm) {
        PageLock newLock = new PageLock(pid);
        PageLock pageLock = ppMap.putIfAbsent(pid, newLock);
        if (pageLock == null) // Indicates that a new pair is inserted into "ppMap"
            pageLock = newLock;
        Set<PageId> holdLockList = new HashSet<>();
        Set<PageId> pageIds = tpMap.putIfAbsent(tid, holdLockList);
        if (pageIds != null)
            holdLockList = pageIds;

        boolean ans;
        if (perm == Permissions.READ_ONLY)
            ans = pageLock.sharedLock(tid);
        else
            ans = pageLock.exclusiveLock(tid);
        if (ans) // Don't forget to change the values in tpMap.
            holdLockList.add(pid);
        return ans;
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
//        if (lockMap.containsKey(p)) {
//            ConcurrentHashMap<TransactionId, LockType> map = lockMap.get(p);
//            return map.containsKey(tid);
//        }
//        return false;
        if (tpMap.containsKey(tid)) {
            return tpMap.get(tid).contains(p);
        }
        return false;
    }

    /**
     * Get the pages that have EX_LOCK.
     */
    public Set<PageId> getEXLockPages(TransactionId tid) {
        Set<PageId> pageIds = tpMap.get(tid);
        return pageIds.stream().filter(pageId ->
                ppMap.get(pageId).ex_tid != null
        ).collect(Collectors.toSet());
    }

    /**
     * Get all PageId in the lockManager.
     */
    public Set<PageId> getPageIds(TransactionId tid) {
        return tpMap.getOrDefault(tid, new HashSet<>());
    }

    /**
     * Release the locks of a transaction on all the pages
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
//        Set<PageId> sets = lockMap.keySet();
//        for (PageId pid : sets) {
//            releaseLockOnPage(tid, pid);
//        }
        Set<PageId> pageIds = tpMap.get(tid);
        if (pageIds != null) {
            for (PageId pid : pageIds) {
                PageLock pageLock = ppMap.get(pid);
                pageLock.releaseLock(tid);
            }
        }
    }

    /**
     * Release the locks on one page
     */
    public synchronized void releaseLockOnPage(TransactionId tid, PageId pid) {
//        if (holdsLock(tid, pid)) {
//            ConcurrentHashMap<TransactionId, LockType> map = lockMap.get(pid);
//            map.remove(tid);
//            if (map.size() == 0) {
//                // Remember to remove the map if no locks remain on this page
//                lockMap.remove(pid);
//            }
//            // !!! Wakeup other threads
//            this.notifyAll();
//        }
        PageLock pageLock = ppMap.get(pid);
        pageLock.releaseLock(tid);
        tpMap.get(tid).remove(pid);
    }

    private static class PageLock {
        private volatile TransactionId ex_tid; // The current transaction which holds an EX_LOCK
        private final Set<TransactionId> shares; // The current transactions holding SHARED_LOCKS
        private final PageId pageId;

        public PageLock(PageId pid) {
            this.pageId = pid;
            this.shares = new HashSet<>();
        }

        public boolean sharedLock(TransactionId tid) {
            if (ex_tid == null) {
                // No EX_LOCK on this page
                shares.add(tid);
                return true;
            }
            return Objects.equals(tid, ex_tid);
        }

        public boolean exclusiveLock(TransactionId tid) {
            if (ex_tid == null && shares.size() == 0) {
                ex_tid = tid;
                return true;
            }
            if (ex_tid == null && shares.size() == 1 && shares.contains(tid)) {
                // update the SHARED_LOCK to EX_LOCK
                shares.remove(tid);
                ex_tid = tid;
                return true;
            }
            return Objects.equals(tid, ex_tid);
        }

        public synchronized void releaseLock(TransactionId tid) {
            if (ex_tid == null)
                shares.remove(tid);
            else if (Objects.equals(ex_tid, tid))
                ex_tid = null;
        }

        // Debugging Info, used in LogFiles
        @Override
        public String toString() {
            if (ex_tid != null) {
                return pageId.toString() + ex_tid.getId();
            } else {
                return pageId.toString();
            }
        }
    }
}
