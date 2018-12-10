package simpledb;

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
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    private Map<PageId, Page> bufferPoolMap;
    private Stack<PageId> LRUdata;
    private TransactionHandler handler;
    private int numPages;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    private PageId MRUPage;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.bufferPoolMap = new ConcurrentHashMap<>();
        this.handler = new TransactionHandler();
      /*  bufferPoolMap = new LinkedHashMap<PageId, Page>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                if (size() == numPages) {
                    LRUdata.clear();
                    LRUdata.put(eldest.getKey(), eldest.getValue());
                }
                return false;
            }
        };*/
    }

    public int getNumPages() {
        return numPages;
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
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        if (bufferPoolMap.size() == this.numPages) {
            evictPage();
        }

        if (!(bufferPoolMap.containsKey(pid))) {
            Page p = Database
                    .getCatalog()
                    .getDatabaseFile(pid.getTableId())
                    .readPage(pid);
            p.setBeforeImage();
            bufferPoolMap.put(pid, p);
        }

        MRUPage = pid;
        handler.getHandle(tid, pid, perm);
        return bufferPoolMap.get(pid);

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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
        handler.removeHandle(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
        return handler.checkForHandles(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
        if (handler.iterateoverset(tid) == null) {
            return;
        }
        if (commit) {
            Iterator<PageId> pageIdIterator = handler.iterateoverset(tid);
            while (pageIdIterator.hasNext()) {
                PageId pid = pageIdIterator.next();
                Page p = this.bufferPoolMap.get(pid);
                if (p != null) {
                    flushPage(pid);
                    bufferPoolMap.get(pid).setBeforeImage();
                }
            }
        } else {
            Iterator<PageId> pageIdIterator = handler.iterateoverset(tid);
            while (pageIdIterator.hasNext()) {
                PageId pid = pageIdIterator.next();
                if (bufferPoolMap.containsKey(pid)) {
                    Page p = bufferPoolMap.get(pid);
                    if (p.isDirty() != null &&
                            p.isDirty().equals(tid)) {
                        bufferPoolMap.put(pid, p.getBeforeImage());
                    }
                }
            }
        }

        handler.removeAllHandles(tid);
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
        DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = dbfile.insertTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            bufferPoolMap.put(page.getId(), page);
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
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pages = f.deleteTuple(tid, t);
        for (Page p : pages) {
            p.markDirty(true, tid);
            bufferPoolMap.put(p.getId(), p);
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
        bufferPoolMap.forEach((pageId, page) -> {
            try {
                flushPage(pageId);
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
        bufferPoolMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = bufferPoolMap.get(pid);
        if (page != null) {
            TransactionId transactionId = page.isDirty();
            if (transactionId != null) {
                Database.getLogFile().logWrite(transactionId, page.getBeforeImage(), page);
                Database.getLogFile().force();
            }
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(this.bufferPoolMap.get(pid));
            page.markDirty(false, transactionId);
        }

    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
        Iterator<PageId> pageIdIterator = handler.iterateoverset(tid);
        while (pageIdIterator.hasNext()) {
            flushPage(pageIdIterator.next());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId pageToBeEvicted = MRUPage;
        Iterator<PageId> pageIdIterator = bufferPoolMap.keySet().iterator();
        while (pageIdIterator.hasNext() && (this.bufferPoolMap.get(pageToBeEvicted).isDirty() != null)) {
            pageToBeEvicted = pageIdIterator.next();
        }
        if (this.bufferPoolMap.get(pageToBeEvicted).isDirty() != null) {
            throw new DbException("No pages avilable to evict");
        }
        discardPage(pageToBeEvicted);
    }

}
