package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private Type[] typeAr;
    String[] fieldAr;
    private ArrayList<TDItem> tdItems = new ArrayList<>();
 
    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;


        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        for (int i = 0; i < typeAr.length; i++) {
            TDItem field = new TDItem(typeAr[i], fieldAr[i]);
            tdItems.add(field);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        for (Type aTypeAr : typeAr) {
            TDItem field = new TDItem(aTypeAr, "");
            tdItems.add(field);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {

        if (i > tdItems.size()) {
            throw new NoSuchElementException("Invalid field reference for Field, Field not found");
        }
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i > tdItems.size()) {
            throw new NoSuchElementException("Invalid field reference for Field, Field not found");
        }
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (TDItem field : tdItems) {
            int f = tdItems.indexOf(field);
            if (field.fieldName != null) {
                if (field.fieldName.equals(name)) {
                    return f;
                }
            } else {
                if (name == null) {
                    return f;
                }
            }
        }
        throw new NoSuchElementException("Invalid field reference for Field, Field not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int length = 0;
        for (TDItem field : tdItems) {
            length += field.fieldType.getLen();
        }
        return length;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
        String[] fieldAr = new String[td1.numFields() + td2.numFields()];
        int i;

        for (i = 0; i < td1.numFields(); i++) {
            typeAr[i] = td1.getFieldType(i);
            fieldAr[i] = td1.getFieldName(i);
        }

        for (int j = 0; j < td2.numFields(); j++) {
            typeAr[i] = td2.getFieldType(j);
            fieldAr[i] = td2.getFieldName(j);
            i++;
        }
        return new TupleDesc(typeAr, fieldAr);

    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) return true;
        if (o instanceof TupleDesc) {
            TupleDesc obj = (TupleDesc) o;
            if ((obj.getSize() == this.getSize())) {
                for (int i = 0; i < obj.numFields() - 1; i++) {
                    if (!this.getFieldType(i).equals(obj.getFieldType(i))) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Objects.hash(typeAr, fieldAr);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < this.numFields(); i++) {
            str.append(this.getFieldType(i).toString()).append("(").append(this.getFieldName(i)).append(")").append(",");
        }
        return str.toString();
    }
}
