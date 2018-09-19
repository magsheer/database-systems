package simpledb;

import java.util.Iterator;

public class HeapFileIterator extends AbstractDbFileIterator{
	
	private HeapFile heapFile;
	private TransactionId tid;
	private Iterator<Tuple> tupleIterator;
	private int pageIndex;

	public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
		this.tid = tid;
		this.heapFile = heapFile;
		this.pageIndex = 0;
	}

	@Override
	public void open() throws DbException, TransactionAbortedException {
		//opens the iterator

		HeapPageId pid = new HeapPageId(heapFile.getId(), pageIndex++);
		Database.getBufferPool();
		//getPage is returning null
		HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid,pid,Permissions.READ_ONLY);
		tupleIterator = heapPage.iterator();
				
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
		if(tupleIterator.hasNext())
			return tupleIterator.next();
		//if there are more pages in the same file
		if(pageIndex < heapFile.numPages()) {
			HeapPageId pid = new HeapPageId(heapFile.getId(), pageIndex++);
			Database.getBufferPool();
			HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid,pid,Permissions.READ_ONLY);
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
		pageIndex = 0;
		tupleIterator = null;
	}
	
}
