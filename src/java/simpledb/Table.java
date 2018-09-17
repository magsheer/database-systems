package simpledb;

import java.util.ArrayList;
import java.util.Objects;

public class Table {
    private DbFile dbFile;
    private String tableName;
    private String primaryKeyField;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Table table = (Table) o;
        return getTableId() == table.getTableId() &&
                Objects.equals(getDbFile(), table.getDbFile()) &&
                Objects.equals(getTableName(), table.getTableName()) &&
                Objects.equals(getPrimaryKeyField(), table.getPrimaryKeyField()) &&
                Objects.equals(getTupleDesc(), table.getTupleDesc());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDbFile(), getTableName(), getPrimaryKeyField(), getTupleDesc(), getTableId());
    }
}
