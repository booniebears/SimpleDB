package simpledb.transaction;

import simpledb.storage.PageId;

public class Lock {
    private TransactionId tid;
    private LockType lockType;
    private PageId pageId;

    public Lock(TransactionId tid, LockType lockType, PageId pageId) {
        this.tid = tid;
        this.lockType = lockType;
        this.pageId = pageId;
    }

    public TransactionId getTid() {
        return tid;
    }

    public void setTid(TransactionId tid) {
        this.tid = tid;
    }

    public LockType getLockType() {
        return lockType;
    }

    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }

    public PageId getPageId() {
        return pageId;
    }

    public void setPageId(PageId pageId) {
        this.pageId = pageId;
    }
}
