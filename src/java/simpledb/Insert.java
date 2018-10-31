package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId t;
    private final TupleDesc td;
    private DbIterator child;
    private final int tableId;
    private boolean inserted;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.inserted = false;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"No of inserted records"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        inserted = false;

    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (inserted) return null;
        int count = 0;
        while (child.hasNext()) {
            Tuple tup = child.next();
            try {
                Database.getBufferPool().insertTuple(t, tableId, tup);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count++;
        }
        Tuple result = new Tuple(td);
        result.setField(0, new IntField(count));
        inserted = true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if (this.child != children[0])
            child = children[0];
    }
}
