package simpledb.storage;

import simpledb.Debug;
import simpledb.common.*;
import simpledb.index.BTreeFile;
import simpledb.index.BTreeLeafPage;
import simpledb.index.BTreePageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private List<Page> pages;
    LRUCache<PageId, Page> LRUCache;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
//        System.out.println("BufferPool: bufferpool construct, capacity: " + numPages);
        pages = new ArrayList<>(numPages);
        LRUCache = new LRUCache<>(numPages);
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

    public Page getNotFullPageFromCache(int tableid) throws DbException {
        Set<PageId> ids = LRUCache.getIds();
        for (PageId id : ids) {
            if (id.getTableId() == tableid) {
                HeapPage page = (HeapPage) getPage(id);
                if (!page.isFull())
                    return page;
            }
        }
        return null;
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        // try to get the lock
//        System.out.println("BufferPool get page: " + pid + " perm: " + perm + " tid: " + tid);
//        System.out.println(LRUCache.toString());
        try {
            Database.getLockManager().acquireLock(tid, pid, perm);
        } catch (DeadlockException e) {
            throw new TransactionAbortedException();
        }

        // get the page by catch or disk
        Page page = LRUCache.get(pid);
        // the removed page
        Page remove = null;
//        System.out.println("BufferPool: " + LRUCache);
        if (page == null) {
//            System.out.println("BufferPool From Disk get page: " + pid + " perm: " + perm + " tid: " + tid);
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            // add the Page to Map
            remove = LRUCache.put(page.getId(), page);
        } else {
//            System.out.println("BufferPool From Catch get page: " + pid + " perm: " + perm + " tid: " + tid);
        }

        // mark the page if it is dirty by the perm
        if (perm == Permissions.READ_WRITE) {
            page.markDirty(true, tid);
        }

        // flush the removed page to disk (NO STEAL)
//        try {
//            if (remove != null && remove.isDirty() != null) flushPage(remove);
//        } catch (IOException e) {
//            throw new DbException("IO Exception");
//        }
        return page;
    }

    // internal use, no lock
    public Page getPage(PageId pid) throws DbException {
//        System.out.println("BufferPool get page: " + pid);
//        System.out.println(LRUCache.toString());
        if (LRUCache.containsKey(pid)) {
//            System.out.println("BufferPool: " + "get from catch, catchSize: " + LRUCache.getSize());
            return LRUCache.get(pid);
        }
//        System.out.println("BufferPool: " + "fail to get from catch");
        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        Page remove = LRUCache.put(page.getId(), page);

        try {
            if (remove != null && remove.isDirty() != null) flushPage(remove);
        } catch (IOException e) {
            throw new DbException("IO Exception");
        }
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
//        System.out.println("BufferPool: unsafeReleasePage " + pid);
        Database.getLockManager().unLockPage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        Set<PageId> markPages = Database.getLockManager().getMarkPages(tid);
        if (markPages == null) {
            Database.getLockManager().removeTxnMark(tid);
            return;
        }
        for (PageId pid : markPages) {
            unsafeReleasePage(tid, pid);
            try {
                flushPage(pid);
            } catch (IOException e) {
                System.out.println("warning, IOException");
            }
        }
        Database.getLockManager().removeTxnMark(tid);
//        System.out.println("transactionComplete " + "tid: " + tid.toString());
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return true;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     * <p>
     * When you commit, you should flush dirty pages associated to the transaction to
     * disk.
     * <p>
     * When you abort, you should revert any changes made by the transaction by
     * restoring the page to its on-disk state.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
//        System.out.println("transactionComplete " + "tid: " + tid.toString());
        if (commit) {
            transactionComplete(tid);
        } else {
            // release all pages in regard to the txn
            // TODO better solution?
            transactionAbort(tid);
        }
    }


    public void transactionAbort(TransactionId tid) {
//        System.out.println("Transaction: abort begin " + tid);
        Set<PageId> markPages = Database.getLockManager().getMarkPages(tid);
        for (PageId pid : markPages) {
            unsafeReleasePage(tid, pid);
            LRUCache.remove(pid);
        }
        Database.getLockManager().removeTxnMark(tid);
//        System.out.println("Transaction: notify " + tid + " " + Database.getLockManager());
//        System.out.println("Transaction: abort end " + tid);
    }


    /**
     * @param pid
     * @throws IOException
     */
    public void revertPageAndFlush(PageId pid) throws IOException {
        Page page;
        try {
            page = Database.getBufferPool().getPage(pid);
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
        page = page.getBeforeImage();
        flushPage(page);
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
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException {

        // if btree, find the right page to insert

//        if (t.getRecordId() != null) {
//            t.getRecordId().getPageId().setTableId(tableId);
//        }
//
//        if (t.getRecordId() != null && t.getRecordId().getPageId().getClass() == BTreePageId.class) {
//            BTreeFile bTreeFile = (BTreeFile) Database.getCatalog().getDatabaseFile(tableId);
//            bTreeFile.insertTuple(tid, t);
//            return;
//        }
//
//        if (t.getRecordId() != null) {
//            Page page = Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
//            if (page.getClass() == HeapPage.class) {
//                HeapPage p = (HeapPage) page;
//                p.insertTuple(t);
//                p.markDirty(true, tid);
//                return;
//            } else if (page.getClass() == BTreeLeafPage.class) {
//                BTreeLeafPage p = (BTreeLeafPage) page;
//                p.insertTuple(t);
//                p.markDirty(true, tid);
//            }
//        }

        // if can't find the page, get page from disk
        Debug.printTxn(tid, "insert begin");
        List<Page> modifiedPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : modifiedPages) {
            LRUCache.put(page.getId(), page);
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
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
//        HeapPage p = (HeapPage) LRUCache.get((t.getRecordId() != null) ? t.getRecordId().getPageId() : null);

        if (t.getRecordId() != null) {
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
//            System.out.println("Before delete: " + p.getUsedSlots());
            p.deleteTuple(t);
            p.markDirty(true, tid);
//            System.out.println("After delete: " + p.getUsedSlots());
            return;
        }
        // if can't find the page, get page from disk
        List<Page> modifiedPages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        for (Page page : modifiedPages) {
            LRUCache.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        Iterator<Page> it = LRUCache.valueIterator();
        while (it.hasNext()) {
            Page p = it.next();
            if (p.isDirty() != null) {
                TransactionId tid = p.isDirty();
//                System.out.println("===flush=== tid: " + tid.toString() + " pageId: " + p.getId());
                Database.getCatalog().getDatabaseFile(p.getId().getTableId()).writePage(p);
                // release the page
                unsafeReleasePage(tid, p.getId());
                it.remove();
            }
        }
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
    public synchronized void removePage(PageId pid) {
        LRUCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
//        System.out.println("BufferPool: flush " + pid);
        Page p = LRUCache.get(pid);
        if (p == null) {
            return;
        }
        Database.getCatalog().getDatabaseFile(p.getId().getTableId()).writePage(p);
        LRUCache.remove(pid);
    }

    private synchronized void flushPage(Page p) throws IOException {
//        System.out.println("BufferPool: flush " + p.getId());
        Database.getCatalog().getDatabaseFile(p.getId().getTableId()).writePage(p);
        LRUCache.remove(p.getId());
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        Set<PageId> markPages = Database.getLockManager().getMarkPages(tid);
        for (PageId pid : markPages) {
            flushPage(pid);
        }
    }


    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     *
     * Do not need, because getPage() already can evict page when cathe is oversize
     *
     */
//    private synchronized void evictPage() throws DbException, IOException {
//    }

}


class PageIdNode {
    public PageIdNode next;
    public PageIdNode pre;
    public PageId pageId;

    PageIdNode(PageId pageId) {
        this.pageId = pageId;
    }
}



