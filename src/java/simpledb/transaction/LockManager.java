package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Record the lock information on each page. The information includes the pageId, TransactionId and
 * the LockType, which can be implemented with a ConcurrentHashMap.
 */
public class LockManager {
    //  locking at page granularity(粒度)
    private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, LockType>> lockMap;
    private static final int X_LOCK_WAIT_TIME = 100;
    private static final int S_LOCK_WAIT_TIME = 100;
    private static final int MAX_RETRY = 3;

    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        if (lockMap.containsKey(p)) {
            ConcurrentHashMap<TransactionId, LockType> map = lockMap.get(p);
            return map.containsKey(tid);
        }
        return false;
    }

    public synchronized boolean acquireLock(PageId pid, TransactionId tid, LockType type, int retry)
            throws InterruptedException {
        // Exceeds the max retry times
        if (retry == MAX_RETRY) return false;

        // 1.If the required page does not have any lock
        if (!lockMap.containsKey(pid)) {
            ConcurrentHashMap<TransactionId, LockType> map = new ConcurrentHashMap<>();
            map.put(tid, type);
            lockMap.put(pid, map);
            return true;
        }
        // If the required page already has lock(s)
        // 2. The TransactionId is the same as the required one
        ConcurrentHashMap<TransactionId, LockType> map = lockMap.get(pid);
        if (map.containsKey(tid)) {
            if (type == map.get(tid)) // No need to update
                return true;
            if (map.get(tid) == LockType.EX_LOCK) // EX_LOCK is the strongest, so no need to update
                return true;
            // If the required one is an EX_LOCK
            if (map.get(tid) == LockType.SHARED_LOCK) {
                // tid is the only Transaction that locks the page
                if (map.size() == 1) {
                    map.put(tid, type);
                    return true;
                }
                // There are other transactions holding SHARED_LOCK, wait first then retry
                else {
                    wait(S_LOCK_WAIT_TIME);
                    acquireLock(pid, tid, type, retry + 1);
                }
            }
            return false;
        }
        // 3. The TransactionId is not the same as the required one
        else {
            // If the required one is an EX_LOCK
            if (type == LockType.EX_LOCK) {
                // wait first, then retry
                wait(X_LOCK_WAIT_TIME);
                acquireLock(pid, tid, type, retry + 1);
            }
            // If the required one is a SHARED_LOCK
            else if (type == LockType.SHARED_LOCK) {
                // If an EX_LOCK exists, wait first, then retry
                if (map.containsValue(LockType.EX_LOCK)) {
                    wait(S_LOCK_WAIT_TIME);
                    acquireLock(pid, tid, type, retry + 1);
                }
                // Otherwise, all the locks are SHARED_LOCK
                else {
                    map.put(tid, type);
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * Release the locks of a transaction on all the pages
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> sets = lockMap.keySet();
        for (PageId pid : sets) {
            releaseLockOnPage(tid, pid);
        }
    }

    /**
     * Release the locks on one page
     */
    public synchronized void releaseLockOnPage(TransactionId tid, PageId pid) {
        if (holdsLock(tid, pid)) {
            ConcurrentHashMap<TransactionId, LockType> map = lockMap.get(pid);
            map.remove(tid);
            if (map.size() == 0) {
                // Remember to remove the map if no locks remain on this page
                lockMap.remove(pid);
            }
            // !!! Wakeup other threads
            this.notifyAll();
        }
    }

}
