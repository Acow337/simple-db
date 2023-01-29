package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * items contain the content of the fields
     */
    private List<TDItem> items;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

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
        return items.iterator();
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
        init();
        int length = typeAr.length;
        for (int i = 0; i < length; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            items.add(item);
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
        init();
        int length = typeAr.length;
        for (int i = 0; i < length; i++) {
            TDItem item = new TDItem(typeAr[i], "unnamed");
            items.add(item);
        }
    }

    public TupleDesc() {
        init();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException();
        }
        return items.get(i).fieldName;
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
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException();
        }
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        int i = 0;
        for (TDItem item : items) {
            if (item.fieldName.equals(name)) {
                return i;
            }
            i++;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int length = 0;
        for (TDItem item : items) {
            if (item.fieldType != null)
                length += item.fieldType.getLen();
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
        Iterator<TDItem> iterator1 = td1.iterator();
        Iterator<TDItem> iterator2 = td2.iterator();
        int size = td1.numFields() + td2.numFields();
        int index = 0;
        Type[] types = new Type[size];
        String[] strings = new String[size];
        while (iterator1.hasNext()) {
            TDItem item = iterator1.next();
            types[index] = item.fieldType;
            strings[index] = item.fieldName;
            index++;
        }
        while (iterator2.hasNext()) {
            TDItem item = iterator2.next();
            types[index] = item.fieldType;
            strings[index] = item.fieldName;
            index++;
        }
        return new TupleDesc(types, strings);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        if (items.size() != ((TupleDesc) o).items.size()) return false;
        Iterator<TDItem> iterator1 = items.iterator();
        Iterator<TDItem> iterator2 = ((TupleDesc) o).iterator();
        while (iterator1.hasNext()) {
            if (!iterator1.next().fieldType.equals(iterator2.next().fieldType)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (TDItem item : items) {
            hashCode ^= item.fieldType.hashCode();
        }
        return hashCode;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (TDItem item : items) {
            sb.append(items.toString());
            sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        return sb.toString();
    }

    /**
     * init the items
     */
    private void init() {
        if (items == null) {
            synchronized (this) {
                if (items == null) {
                    items = new ArrayList<>();
                }
            }
        }
    }

}
