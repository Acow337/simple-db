package simpledb.storage;

import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {

    private int tableId;
    private int pageNum;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableId = tableId;
        pageNum = pgNo;
    }

    public HeapPageId() {
        tableId = -1;
        pageNum = -1;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     * this PageId
     */
    public int getPageNumber() {
        return pageNum;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HeapPageId that = (HeapPageId) o;
        return tableId == that.tableId && pageNum == that.pageNum;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     * the table number and the page number (needed if a PageId is used as a
     * key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    @Override
    public int hashCode() {
        return Objects.hash(tableId, pageNum);
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

    @Override
    public String toString() {
        return "HeapPageId{" +
                "tableId=" + tableId +
                ", pageNum=" + pageNum +
                '}';
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

}
