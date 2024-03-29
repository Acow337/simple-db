package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    boolean isDirty = false;
    TransactionId lastTid;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
//        System.out.println("pageId : " + pid);
//        System.out.println("slot nums : " + numSlots);
//        System.out.println("unusedSlots : " + getNumUnusedSlots());
//        System.out.println("header length : " + header.length);
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     * <p>
     * Specifically, the number of tuples is equal to floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     */
    private int getNumTuples() {
        return (BufferPool.getPageSize() * 8) / (td.getSize() * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * <p>
     * header bytes = ceiling(tuples per page / 8)
     */
    private int getHeaderSize() {
        int x = getNumTuples();
        int y = 8;
        return x / y + (x % y != 0 ? 1 : 0);
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        if (getNumUnusedSlots() == tuples.length)
            throw new DbException("page is empty");
        int i = t.getRecordId().getTupleNumber();
//        System.out.println("Delete: origin tuple: " + tuples[i].toValueString() + " delete tuple: " + t.toValueString());
        if (!isSlotUsed(i)) {
//            System.out.println("origin tuple: " + tuples[i].toValueString() + " delete tuple: " + t.toValueString() + " !isSlotUsed(i)");
            return;
        }
        if (!tuples[i].equals(t)) {
            System.out.println("origin tuple: " + tuples[i].toValueString() + " delete tuple: " + t.toValueString() + " !tuples[i].equals(t)");
//            throw new DbException("");
        }
        int a = i / 8;
        int b = i % 8;
        header[a] = (byte) (header[a] & (~(0x1 << b)));
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        if (isFull())
            throw new DbException("the page is full");
        if (!td.equals(t.getTupleDesc()))
            throw new DbException("tupledesc is mismatch");
//        System.out.println("before: " + isSlotUsed(t.getRecordId().getTupleNumber()));
        int i = t.getRecordId() != null ? t.getRecordId().getTupleNumber() : getUnusedSlot();
        if (isSlotUsed(i)) {
            i = getUnusedSlot();
            t.getRecordId().setTupleNum(i);
        }
        t.setRecordId(new RecordId(pid, i));
        tuples[i] = t;
        markSlotUsed(i, true);
//        System.out.println("after: " + isSlotUsed(t.getRecordId().getTupleNumber()));
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
//        System.out.println("HeapPage: " + pid + " get dirty");
        isDirty = dirty;
        lastTid = isDirty ? tid : null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return lastTid;
    }

    /**
     * Returns the number of unused (i.e., empty) slots on this page.
     */
    public int getNumUnusedSlots() {
        int usedNum = 0;
        for (byte b : header) {
            for (int i = 0; i < 8; i++) {
                if (((b >> i) & 1) == 1) {
                    usedNum++;
                }
            }
        }
        return numSlots - usedNum;
    }

    public int getUsedSlots() {
        int usedNum = 0;
        for (byte b : header) {
            for (int i = 0; i < 8; i++) {
                if (((b >> i) & 1) == 1) {
                    usedNum++;
                }
            }
        }
        return usedNum;
    }

    public boolean isFull() {
        return getNumUnusedSlots() == 0;
    }

    public boolean isEmpty() {
        for (byte b : header) {
            if ((b | 0) != 0) return false;
        }
        return true;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int a = i / 8;
        int b = i % 8;
        return ((header[a] >> b) & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        int a = i / 8;
        int b = i % 8;
        header[a] = value ? (byte) (header[a] | (0x1 << b)) : (byte) (header[a] & (~(0x1 << b)));
    }

    private int getUnusedSlot() {
        for (int i = 0; i < header.length; i++) {
            for (int j = 0; j < 8; j++) {
                if (((header[i] >> j) & 1) == 0)
                    return i * 8 + j;
            }
        }
        return -1;
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        return new TupleIterator(this);
    }

    class TupleIterator implements Iterator<Tuple> {
        HeapPage heapPage;
        int headerCur;
        int offset;

        public TupleIterator(HeapPage heapPage) {
            this.heapPage = heapPage;
            headerCur = 0;
            offset = 0;
        }

        public boolean hasNext() {
            for (int i = headerCur; i < header.length; i++) {
                for (int j = offset; j < 8; j++) {
                    if (((header[i] >> j) & 1) == 1) {
                        headerCur = i;
                        offset = j;
                        return true;
                    }
                }
            }
            return false;
        }

        public Tuple next() {
            int index = headerCur * 8 + offset;
            if (index >= heapPage.tuples.length) return null;
            offset++;
            if (offset >= 8) {
                headerCur++;
                offset = 0;
            }
            return heapPage.tuples[index];
        }
    }

}

