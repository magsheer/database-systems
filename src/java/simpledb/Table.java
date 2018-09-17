package simpledb;

import java.util.ArrayList;

public class Table {
    private DbFile dbFile;
    private String tableName;
    private String primaryKeyField;
    private ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    private TupleDesc tupleDesc;
    private int tableId;

    public DbFile getDbFile() {
        return dbFile;
    }

    public void setDbFile(DbFile dbFile) {
        this.dbFile = dbFile;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void setTupleDesc(TupleDesc tupleDesc) {
        this.tupleDesc = tupleDesc;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrimaryKeyField() {
        return primaryKeyField;
    }

    public void setPrimaryKeyField(String primaryKeyField) {
        this.primaryKeyField = primaryKeyField;
    }

    public ArrayList<Tuple> getTuples() {
        return tuples;
    }

    public void setTuples(ArrayList<Tuple> tuples) {
        this.tuples = tuples;
    }
}
