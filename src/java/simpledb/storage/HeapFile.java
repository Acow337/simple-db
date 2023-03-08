package simpledb.storage;

import net.sf.antcontrib.design.Log;
import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc tupleDesc;
    int id;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
        id = file.getAbsoluteFile().hashCode();
        int i = 0;
//        System.out.println(file.length());
//        System.out.println(numPages());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
//        Page page = Database.getBufferPool().getPage(pid);
//        if (page != null) return page;
        int pageSize = BufferPool.getPageSize();
        try {
            FileChannel channel = new RandomAccessFile(file, "r").getChannel();
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, pid.getPageNumber() * pageSize, pageSize).load();
            byte[] result = new byte[pageSize];
            if (buffer.remaining() > 0) {
                buffer.get(result, 0, pageSize);
            }
            buffer.clear();
            channel.close();
            return new HeapPage((HeapPageId) pid, result);
        } catch (IOException e) {
            System.out.println("readPage 出现IO错误 " + " fileSize: " + file.length() + " index: " + pid.getPageNumber() * pageSize);
        }
        return null;
    }

    /**
     * Push the specified page to disk.
     *
     * @param page The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     */
    public void writePage(Page page) throws IOException {
        // if file size is not enough, enlarge it
        while (numPages() <= page.getId().getPageNumber()) {
            appendNewPage();
        }
        try {
            RandomAccessFile f = new RandomAccessFile(file, "rw");
            f.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            f.write(page.getPageData());
            f.close();
        } catch (IOException e) {
            System.out.println("writePage 出现IO错误");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    public void appendNewPage() throws IOException {
        Files.write(file.toPath(),
                HeapPage.createEmptyPageData(),
                StandardOpenOption.APPEND);
    }

    // TODO
    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        List<Page> dirtyPages = new ArrayList<>();
        HeapPage page;
        int pageNo = 0;
        do {
            if (pageNo >= numPages()) {
                appendNewPage();
            }
            page = (HeapPage) Database.getBufferPool().getPage(new HeapPageId(id, pageNo));
            pageNo++;
        } while (page.isFull());
        // Try to get write lock

        try {
            Database.getLockManager().acquireLock(tid, page.pid, Permissions.READ_WRITE);
        } catch (DeadlockException e) {
            throw new TransactionAbortedException();
        }

        // change the tuple's pageId
//        t.setRecordId(new RecordId(new HeapPageId(id, pageNo - 1), t.getRecordId().getTupleNumber()));
        page.insertTuple(t);
        page.markDirty(true, tid);
        dirtyPages.add(page);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException, IOException {
        List<Page> dirtyPages = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        dirtyPages.add(page);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    class HeapFileIterator implements DbFileIterator {
        HeapFile heapFile;
        int pageCur;
        int pageNum;
        HeapPage curPage;
        Iterator<Tuple> curIterator;
        TransactionId tid;
        boolean isOpen;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
            pageCur = 0;
            pageNum = heapFile.numPages();
            isOpen = false;
        }

        public void open() throws DbException, TransactionAbortedException {
            curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(heapFile.id, pageCur), Permissions.READ_ONLY);
            curIterator = curPage.iterator();
            isOpen = true;
        }

        // fetch not from BufferPool ?
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen || pageCur >= pageNum) return false;
            if (curIterator.hasNext()) return true;
            pageCur++;
            if (pageCur >= pageNum) return false;
//            curPage = (HeapPage) Database.getCatalog().getDatabaseFile(id).readPage(new HeapPageId(heapFile.id, pageCur));
            curPage = (HeapPage) Database.getBufferPool().getPage(new HeapPageId(heapFile.id, pageCur));
            if (curPage.isEmpty()) return hasNext();
            curIterator = curPage.iterator();
            return true;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) throw new NoSuchElementException();
            if (!curIterator.hasNext()) {
                pageCur++;
                if (pageCur >= pageNum) throw new NoSuchElementException();
                curPage = (HeapPage) Database.getBufferPool().getPage(new HeapPageId(heapFile.id, pageCur));
//                curPage = (HeapPage) Database.getBufferPool().getPage(null, new HeapPageId(heapFile.id, pageCur), Permissions.READ_ONLY);
                curIterator = curPage.iterator();
            }
            return curIterator.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            pageCur = 0;
            curPage = (HeapPage) Database.getBufferPool().getPage(null, new HeapPageId(heapFile.id, pageCur), Permissions.READ_ONLY);
            curIterator = curPage.iterator();
        }

        public void close() {
            curPage = null;
            curIterator = null;
            isOpen = false;
        }
    }

}

