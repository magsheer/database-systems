package simpledb;

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
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
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
     * you will need to generate this tableid somewhere ensure that each
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
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(f, "r");
            byte[] bytes = HeapPage.createEmptyPageData();
            randomAccessFile.seek((pid.pageNumber() * BufferPool.getPageSize()));
            randomAccessFile.read(bytes, 0, BufferPool.getPageSize());
            randomAccessFile.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.pageNumber()), bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        RandomAccessFile randomAccessFile = new RandomAccessFile(f, "rw");
        randomAccessFile.seek((page.getId().pageNumber() * BufferPool.getPageSize()));
        randomAccessFile.write(page.getPageData(), 0, BufferPool.getPageSize());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(f.length() / (float)BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1|lab2
        HeapPage hp = getPageWithEmptySlot(tid);
        if (hp != null) {
            hp.insertTuple(t);
            return new ArrayList<>(Collections.singletonList(hp));
        } else {
            HeapPageId heapPageId = new HeapPageId(this.getId(), this.numPages());
            HeapPage pg = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            pg.insertTuple(t);
            RandomAccessFile randomAccessFile = new RandomAccessFile(f, "rw");
            randomAccessFile.seek((this.numPages() * BufferPool.getPageSize()));
            randomAccessFile.write(pg.getPageData(), 0, BufferPool.getPageSize());
            randomAccessFile.close();
            return new ArrayList<>(Collections.singletonList(pg));
        }

    }

    private HeapPage getPageWithEmptySlot(TransactionId t) throws TransactionAbortedException, DbException {
        for (int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(t, pid, Permissions.READ_WRITE);
            if (p.getNumEmptySlots() > 0)
                return p;
        }
        return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1|lab2
        PageId pid = t.getRecordId().getPageId();
        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        hp.deleteTuple(t);
        return new ArrayList<>(Collections.singletonList(hp));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

}

