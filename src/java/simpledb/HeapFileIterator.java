package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

//public class HeapFileIterator extends AbstractDbFileIterator{
//
//	@Override
//	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//		// TODO Auto-generated method stub
//		return super.next();
//	}
//	
//	@Override
//	public boolean hasNext() throws DbException, TransactionAbortedException {
//		// TODO Auto-generated method stub
//		return super.hasNext();
//	}
//	
//	@Override
//	public void open() throws DbException, TransactionAbortedException {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void rewind() throws DbException, TransactionAbortedException {
//		// TODO Auto-generated method stub
//		
//	}
//	
//	@Override
//	public void close() {
//		// TODO Auto-generated method stub
//		super.close();
//	}
//
//	@Override
//	protected Tuple readNext() throws DbException, TransactionAbortedException {
//		// TODO Auto-generated method stub
//		return null;
//	}
//}


public class HeapFileIterator extends AbstractDbFileIterator {

    private HeapFile heapFile;
    private TransactionId tid;
    private Iterator<Tuple> tupleIterator;
    private int pageId;

    public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
        this.tid = tid;
        this.heapFile = heapFile;
        this.pageId = -1;
        this.tupleIterator = null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        //opens the iterator
        pageId = 0;
//        HeapPageId pid = new HeapPageId(heapFile.getId(), pageId++);
//        //getPage is returning null
//        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY);
//        tupleIterator = heapPage.iterator();

    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // resets iterator to the start
        close();
        open();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        // if the iterator hasnt' finished
        if (tupleIterator != null && tupleIterator.hasNext())
            return tupleIterator.next();
        //if there are more pages in the same file
        if (pageId < heapFile.numPages()) {
            HeapPageId pid = new HeapPageId(heapFile.getId(), pageId++);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY);
            tupleIterator = heapPage.iterator();
            //have to fix return statement
            return readNext();
        }
        //reached end of file
        else
            return null;
    }

    @Override
    public void close() {
        super.close();
        pageId = -1;
        tupleIterator = null;
    }

}
