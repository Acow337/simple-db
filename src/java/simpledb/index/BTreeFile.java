package simpledb.index;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.execution.IndexPredicate;
import simpledb.execution.Predicate.Op;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * BTreeFile is an implementation of a DbFile that stores a B+ tree.
 * Specifically, it stores a pointer to a root page,
 * a set of internal pages, and a set of leaf pages, which contain a collection of tuples
 * in sorted order. BTreeFile works closely with BTreeLeafPage, BTreeInternalPage,
 * and BTreeRootPtrPage. The format of these pages is described in their constructors.
 *
 * @author Becca Taft
 * @see BTreeLeafPage#BTreeLeafPage
 * @see BTreeInternalPage#BTreeInternalPage
 * @see BTreeHeaderPage#BTreeHeaderPage
 * @see BTreeRootPtrPage#BTreeRootPtrPage
 */
public class BTreeFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int tableid;
    private final int keyField;

    /**
     * Constructs a B+ tree file backed by the specified file.
     *
     * @param f   - the file that stores the on-disk backing store for this B+ tree
     *            file.
     * @param key - the field which index is keyed on
     * @param td  - the tuple descriptor of tuples in the file
     */
    public BTreeFile(File f, int key, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.keyField = key;
        this.td = td;
    }

    /**
     * Returns the File backing this BTreeFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this BTreeFile. Implementation note:
     * you will need to generate this tableid somewhere and ensure that each
     * BTreeFile has a "unique id," and that you always return the same value for
     * a particular BTreeFile. We suggest hashing the absolute file name of the
     * file underlying the BTreeFile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this BTreeFile.
     */
    public int getId() {
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    private void printPageMaxMin(BTreeInternalPage page) {
        Iterator<BTreeEntry> iterator = page.iterator();
        BTreeEntry entry = null;
        BTreeEntry first = null;
        while (iterator.hasNext()) {
            entry = iterator.next();
            if (first == null)
                first = entry;
        }
        System.out.println("first: " + first + " last: " + entry);
    }

    private String getPageMaxMin(BTreePageId pageId) throws DbException {
//        if (true)
//            return "";

        BTreeLeafPage page = (BTreeLeafPage) Database.getBufferPool().getPage(pageId);
//        BTreeLeafPage page=(BTreeLeafPage) Database.getBufferPool().getPage(null,pageId,Permissions.READ_ONLY);
        Iterator<Tuple> iterator = page.iterator();
        Integer max = null;
        Integer min = null;
        Tuple tuple = null;
        while (iterator.hasNext()) {
            tuple = iterator.next();
            IntField field = (IntField) tuple.getField(keyField);
            if (min == null)
                min = field.getValue();
        }
        if (tuple != null)
            max = ((IntField) tuple.getField(keyField)).getValue();
        return "(L: " + min + ", No: " + page.pid.getPageNumber() + ", R: " + max + ") ";
    }


    /**
     * Read a page from the file on disk. This should not be called directly
     * but should be called from the BufferPool via getPage()
     *
     * @param pid - the id of the page to read from disk
     * @return the page constructed from the contents on disk
     */
    public Page readPage(PageId pid) {
        BTreePageId id = (BTreePageId) pid;
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
            if (id.pgcateg() == BTreePageId.ROOT_PTR) {
                byte[] pageBuf = new byte[BTreeRootPtrPage.getPageSize()];
                int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BTreeRootPtrPage.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read "
                            + BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                return new BTreeRootPtrPage(id, pageBuf);
            } else {
                byte[] pageBuf = new byte[BufferPool.getPageSize()];
                if (bis.skip(BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) !=
                        BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) {
                    throw new IllegalArgumentException(
                            "Unable to seek to correct place in BTreeFile");
                }
                int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BufferPool.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read "
                            + BufferPool.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                if (id.pgcateg() == BTreePageId.INTERNAL) {
                    return new BTreeInternalPage(id, pageBuf, keyField);
                } else if (id.pgcateg() == BTreePageId.LEAF) {
                    return new BTreeLeafPage(id, pageBuf, keyField);
                } else { // id.pgcateg() == BTreePageId.HEADER
                    return new BTreeHeaderPage(id, pageBuf);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Close the file on success or error
        // Ignore failures closing the file
    }

    /**
     * Write a page to disk.  This should not be called directly but should
     * be called from the BufferPool when pages are flushed to disk
     *
     * @param page - the page to write to disk
     */
    public void writePage(Page page) throws IOException {
        BTreePageId id = (BTreePageId) page.getId();

        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        if (id.pgcateg() == BTreePageId.ROOT_PTR) {
            rf.write(data);
            rf.close();
        } else {
            rf.seek(BTreeRootPtrPage.getPageSize() + (long) (page.getId().getPageNumber() - 1) * BufferPool.getPageSize());
            rf.write(data);
            rf.close();
        }
    }

    /**
     * Returns the number of pages in this BTreeFile.
     */
    public int numPages() {
        // we only ever write full pages
        return (int) ((f.length() - BTreeRootPtrPage.getPageSize()) / BufferPool.getPageSize());
    }

    /**
     * Returns the index of the field that this B+ tree is keyed on
     */
    public int keyField() {
        return keyField;
    }

    /**
     * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
     * the left-most page possibly containing the key field f. It locks all internal
     * nodes along the path to the leaf node with READ_ONLY permission, and locks the
     * leaf node with permission perm.
     * <p>
     * If f is null, it finds the left-most leaf page -- used for the iterator
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - the current page being searched
     * @param perm       - the permissions with which to lock the leaf page
     * @param f          - the field to search for
     * @return the left-most leaf page possibly containing the key field f
     */
    private BTreeLeafPage findLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm,
                                       Field f)
            throws DbException, TransactionAbortedException {

        BTreePage page = (BTreePage) Database.getBufferPool().getPage(tid, pid, perm);
        // TODO: some code goes here

        if (f == null) {
            if (page.getId().pgcateg() == BTreePageId.INTERNAL) {
                BTreeInternalPage internalPage = (BTreeInternalPage) page;
                Iterator<BTreeEntry> iterator = internalPage.iterator();
                if (iterator.hasNext()) {
                    BTreeEntry entry = iterator.next();
                    if (entry.getLeftChild() != null) {
                        return findLeafPage(tid, dirtypages, entry.getLeftChild(), perm, f);
                    } else {
                        return findLeafPage(tid, dirtypages, entry.getRightChild(), perm, f);
                    }
                } else {
                    System.out.println("error");
                }
            } else if (page.getId().pgcateg() == BTreePageId.LEAF) {
                return (BTreeLeafPage) page;
            }
        }

        if (page.getId().pgcateg() == BTreePageId.INTERNAL) {
            BTreeInternalPage internalPage = (BTreeInternalPage) page;
            Iterator<BTreeEntry> iterator = internalPage.iterator();
            BTreeEntry pre = null;
            while (iterator.hasNext()) {
                BTreeEntry entry = iterator.next();
                if (pre == null && f.compare(Op.LESS_THAN_OR_EQ, entry.getKey())) {
                    return findLeafPage(tid, dirtypages, entry.getLeftChild(), perm, f);
                } else if (pre != null && f.compare(Op.GREATER_THAN_OR_EQ, pre.getKey()) && f.compare(Op.LESS_THAN_OR_EQ, entry.getKey())) {
                    // research the pre's right child and the entry's left child
                    return findLeafPage(tid, dirtypages, pre.getRightChild(), perm, f);
                }
                pre = entry;
            }
            return findLeafPage(tid, dirtypages, pre.getRightChild(), perm, f);
        } else if (page.getId().pgcateg() == BTreePageId.LEAF) {
            return (BTreeLeafPage) page;
        } else {
            throw new RuntimeException();
        }
    }

    /**
     * Convenience method to find a leaf page when there is no dirtypages HashMap.
     * Used by the BTreeFile iterator.
     *
     * @param tid - the transaction id
     * @param pid - the current page being searched
     * @param f   - the field to search for
     * @return the left-most leaf page possibly containing the key field f
     * @see #findLeafPage(TransactionId, Map, BTreePageId, Permissions, Field)
     */
    BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid,
                               Field f)
            throws DbException, TransactionAbortedException {
        return findLeafPage(tid, new HashMap<>(), pid, Permissions.READ_ONLY, f);
    }

    /**
     * Split a leaf page to make room for new tuples and recursively split the parent node
     * as needed to accommodate a new entry. The new entry should have a key matching the key field
     * of the first tuple in the right-hand page (the key is "copied up"), and child pointers
     * pointing to the two leaf pages resulting from the split.  Update sibling pointers and parent
     * pointers as needed.
     * <p>
     * Return the leaf page into which a new tuple with key field "field" should be inserted.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the leaf page to split
     * @param field      - the key field of the tuple to be inserted after the split is complete. Necessary to know
     *                   which of the two pages to return.
     * @return the leaf page into which the new tuple should be inserted
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
     */
    public BTreeLeafPage splitLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page, Field field)
            throws DbException, IOException, TransactionAbortedException {
        // Split the leaf page by adding a new page on the right of the existing
        // page and moving half of the tuples to the new page.  Copy the middle key up
        // into the parent page, and recursively split the parent as needed to accommodate
        // the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
        // the sibling pointers of all the affected leaf pages.  Return the page into which a
        // tuple with the given key field should be inserted.
        BTreeLeafPage newPage = (BTreeLeafPage) getEmptyPage(tid, dirtypages, BTreePageId.LEAF);
        System.out.println("BTreeFile: splitLeafPage: " + page.pid + " newPage: " + newPage.pid);

        if (page.pid.pgcateg() == BTreePageId.ROOT_PTR) {
            System.out.println("ROOT: " + page.pid);
        }

        Field midKey = null;
        Iterator<Tuple> iterator = page.iterator();
        int num = page.getNumTuples();
        int i = 0;

        // move the half tuples to new page and add to dirtyPages
        int c = 0;
        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            System.out.println("iterator: " + ((IntField) t.getField(keyField)).getValue() + " page: " + page.pid);
            if (i == num / 2) midKey = t.getField(keyField);
            if (i >= num / 2) {
//                System.out.println("move: " + ((IntField)t.getField(keyField)).getValue());
                page.deleteTuple(t);
                newPage.insertTuple(t);
                c++;
            }
            i++;
        }
        dirtypages.put(page.getId(), page);
        dirtypages.put(newPage.getId(), newPage);
        System.out.println("moveNum: " + c + ", newPage: " + getPageMaxMin(newPage.getId()));

        // update the sibling pointers
        if (page.getRightSiblingId() == null) {
            page.setRightSiblingId(newPage.pid);
            newPage.setLeftSiblingId(page.pid);
        } else {
            BTreeLeafPage formerRightSibling = (BTreeLeafPage) Database.getBufferPool().getPage(tid, page.getRightSiblingId(), Permissions.READ_ONLY);
            formerRightSibling.setLeftSiblingId(newPage.pid);
            page.setRightSiblingId(newPage.pid);
            newPage.setLeftSiblingId(page.pid);
            newPage.setRightSiblingId(formerRightSibling.pid);
            dirtypages.put(formerRightSibling.getId(), formerRightSibling);
        }

        // Copy the middle key up into the parent page    getParentWithEmtpySlots() will be useful here.
        BTreeInternalPage parentPage = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), midKey);
        page.setParentId(parentPage.getId());
        newPage.setParentId(parentPage.getId());

        // insert the middle key to parent and add to dirtyPages
        BTreeEntry newEntry = new BTreeEntry(midKey, page.pid, newPage.pid);
        System.out.println("BTreeFile: leaf: insert to parent, entry: " + newEntry);
        printPageMaxMin(parentPage);
        parentPage.insertEntry(newEntry);
        System.out.println("after insert");
        printPageMaxMin(parentPage);

        dirtypages.put(parentPage.getId(), parentPage);

        System.out.println("parentId: " + page.getParentId());
        if (page.getParentId().pgcateg() == BTreePageId.ROOT_PTR) {
            System.out.println("ROOT: " + page.pid);
        }

        System.out.println("insert to: " + (midKey.compare(Op.LESS_THAN_OR_EQ, field) ? newPage.pid : page.pid));

        // Return the page into which a tuple with the given key field should be inserted.
        return midKey.compare(Op.LESS_THAN_OR_EQ, field) ? newPage : page;
    }

    /**
     * Split an internal page to make room for new entries and recursively split its parent page
     * as needed to accommodate a new entry. The new entry for the parent should have a key matching
     * the middle key in the original internal page being split (this key is "pushed up" to the parent).
     * The child pointers of the new parent entry should point to the two internal pages resulting
     * from the split. Update parent pointers as needed.
     * <p>
     * Return the internal page into which an entry with key field "field" should be inserted
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the internal page to split
     * @param field      - the key field of the entry to be inserted after the split is complete. Necessary to know
     *                   which of the two pages to return.
     * @return the internal page into which the new entry should be inserted
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public BTreeInternalPage splitInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
                                               BTreeInternalPage page, Field field)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        //
        // Split the internal page by adding a new page on the right of the existing
        // page and moving half of the entries to the new page.  Push the middle key up
        // into the parent page, and recursively split the parent as needed to accommodate
        // the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
        // the parent pointers of all the children moving to the new page.  updateParentPointers()
        // will be useful here.  Return the page into which an entry with the given key field
        // should be inserted.
        System.out.println("BTreeFile: splitInternalPage: " + page.getId());

        BTreeInternalPage newPage = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
        int i = 0;
        int num = page.getNumEntries();
        BTreeEntry midEntry = null;
        Iterator<BTreeEntry> iterator = page.iterator();
        int mid = num / 2;
        while (iterator.hasNext()) {
            BTreeEntry entry = iterator.next();
            if (i == mid) {
                midEntry = new BTreeEntry(entry.getKey(), page.pid, newPage.pid);
                page.deleteKeyAndRightChild(entry);
            }
            if (i > mid) {
                page.deleteKeyAndRightChild(entry);
                newPage.insertEntry(entry);
            }
            i++;
        }
        dirtypages.put(page.getId(), page);
        dirtypages.put(newPage.getId(), newPage);

        System.out.println("internalpage size: " + i);
        BTreeInternalPage parentPage = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), field);
        System.out.println("BTreeFile: internal: insert to parent: " + parentPage.getId() + " entry: " + midEntry);

        printPageMaxMin(parentPage);
        parentPage.insertEntry(midEntry);
        dirtypages.put(parentPage.getId(), parentPage);

        // update parentId

        // Don't forget to update
        // the parent pointers of all the children moving to the new page.  updateParentPointers()
        // will be useful here.
        updateParentPointers(tid, dirtypages, page);
        updateParentPointers(tid, dirtypages, newPage);

        System.out.println("parentId: " + page.getParentId());
        if (page.getParentId().pgcateg() == BTreePageId.ROOT_PTR) {
            System.out.println("ROOT: " + page.pid);
        }

        return midEntry.getKey().compare(Op.LESS_THAN_OR_EQ, field) ? newPage : page;
    }

    /**
     * Method to encapsulate the process of getting a parent page ready to accept new entries.
     * This may mean creating a page to become the new root of the tree, splitting the existing
     * parent page if there are no empty slots, or simply locking and returning the existing parent page.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param parentId   - the id of the parent. May be an internal page or the RootPtr page
     * @param field      - the key of the entry which will be inserted. Needed in case the parent must be split
     *                   to accommodate the new entry
     * @return the parent page, guaranteed to have at least one empty slot
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #splitInternalPage(TransactionId, Map, BTreeInternalPage, Field)
     */
    private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, Map<PageId, Page> dirtypages,
                                                      BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {

        BTreeInternalPage parent = null;

        // create a parent node if necessary
        // this will be the new root of the tree
        if (parentId.pgcateg() == BTreePageId.ROOT_PTR) {
            parent = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
            System.out.println("BTreeFile: create a new page, id: " + parent.getId());

            // update the root pointer
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages,
                    BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            BTreePageId prevRootId = rootPtr.getRootId(); //save prev id before overwriting.
            rootPtr.setRootId(parent.getId());

            // update the previous root to now point to this new root.
            BTreePage prevRootPage = (BTreePage) getPage(tid, dirtypages, prevRootId, Permissions.READ_WRITE);
            prevRootPage.setParentId(parent.getId());
        } else {
            // lock the parent page
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId,
                    Permissions.READ_WRITE);
        }

        // split the parent if needed
        if (parent.getNumEmptySlots() == 0) {
            parent = splitInternalPage(tid, dirtypages, parent, field);
        }

        return parent;

    }

    /**
     * Helper function to update the parent pointer of a node.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - id of the parent node
     * @param child      - id of the child node to be updated with the parent pointer
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private void updateParentPointer(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, BTreePageId child)
            throws DbException, TransactionAbortedException {

        BTreePage p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_ONLY);

        if (!p.getParentId().equals(pid)) {
            p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_WRITE);
            p.setParentId(pid);
        }
        dirtypages.put(p.getId(), p);
    }

    /**
     * Update the parent pointer of every child of the given page so that it correctly points to
     * the parent
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the parent page
     * @throws DbException
     * @throws TransactionAbortedException
     * @see # updateParentPointer(TransactionId, HashMap, BTreePageId, BTreePageId)
     */
    private void updateParentPointers(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page)
            throws DbException, TransactionAbortedException {
        Iterator<BTreeEntry> it = page.iterator();
        BTreePageId pid = page.getId();
        BTreeEntry e = null;
        while (it.hasNext()) {
            e = it.next();
            updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
        }
        if (e != null) {
            updateParentPointer(tid, dirtypages, pid, e.getRightChild());
        }
    }

    /**
     * Method to encapsulate the process of locking/fetching a page.  First the method checks the local
     * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.
     * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since
     * presumably they will soon be dirtied by this transaction.
     * <p>
     * This method is needed to ensure that page updates are not lost if the same pages are
     * accessed multiple times.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - the id of the requested page
     * @param perm       - the requested permissions on the page
     * @return the requested page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    Page getPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm)
            throws DbException, TransactionAbortedException {
        if (dirtypages.containsKey(pid)) {
            return dirtypages.get(pid);
        } else {
            Page p = Database.getBufferPool().getPage(tid, pid, perm);
            if (perm == Permissions.READ_WRITE) {
                dirtypages.put(pid, p);
            }
            return p;
        }
    }

    /**
     * Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
     * May cause pages to split if the page where tuple t belongs is full.
     *
     * @param tid - the transaction id
     * @param t   - the tuple to insert
     * @return a list of all pages that were dirtied by this operation. Could include
     * many pages since parent pointers will need to be updated when an internal node splits.
     * @see #splitLeafPage(TransactionId, Map, BTreeLeafPage, Field)
     */
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        Map<PageId, Page> dirtypages = new HashMap<>();

        // get a read lock on the root pointer page and use it to locate the root page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId rootId = rootPtr.getRootId();

        if (rootId == null) { // the root has just been created, so set the root pointer to point to it
            rootId = new BTreePageId(tableid, numPages(), BTreePageId.LEAF);
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            rootPtr.setRootId(rootId);
        }

        // find and lock the left-most leaf page corresponding to the key field,
        // and split the leaf page if there are no more slots available
        BTreeLeafPage leafPage = findLeafPage(tid, dirtypages, rootId, Permissions.READ_WRITE, t.getField(keyField));
        if (leafPage.getNumEmptySlots() == 0) {
//            System.out.println("BTreeFile: " + t.getRecordId().getPageId() + " split page");
            leafPage = splitLeafPage(tid, dirtypages, leafPage, t.getField(keyField));
        }

        // insert the tuple into the leaf page
        leafPage.insertTuple(t);

        return new ArrayList<>(dirtypages.values());
    }

    //TODO a method to print the B+Tree
    public void printTree() throws DbException, IOException, TransactionAbortedException {
        if (true) {
            return;
        }

        Map<PageId, Page> dirtypages = new HashMap<>();
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(BTreeRootPtrPage.getId(tableid));
        BTreePageId rootId = rootPtr.getRootId();
        Page page = Database.getBufferPool().getPage(rootId);
        System.out.println("=======print tree=======");
        System.out.println("Get Class: " + page.getClass());
        Deque<BTreeEntry> deque = new LinkedList<>();
        BTreePageId preId = null;
        if (page.getClass() == BTreeLeafPage.class) {
            BTreeLeafPage leafPage = (BTreeLeafPage) page;
            System.out.println(leafPage.pid);
        } else if (page.getClass() == BTreeInternalPage.class) {
            BTreeInternalPage internalPage = (BTreeInternalPage) page;
            Iterator<BTreeEntry> iterator = internalPage.iterator();
            StringBuilder sb = new StringBuilder();
            int len = 0;
            // init the deque
            while (iterator.hasNext()) {
                BTreeEntry entry = iterator.next();
                deque.offer(entry);
                len++;
            }
            // print the internal page
            while (!deque.isEmpty()) {
                Page p = Database.getBufferPool().getPage(deque.peek().getLeftChild());
                // check if the next page is internal page
                if (p.getClass() != BTreeInternalPage.class) {
                    break;
                }
                for (int i = 0; i < len; i++) {
                    BTreeEntry entry = deque.poll();
                    sb.append(entry.toString() + " ");
                    if (entry.getLeftChild() != null && !entry.getLeftChild().equals(preId)) {
                        addEntryToDeque(deque, entry.getLeftChild());
                        preId = entry.getLeftChild();
                    }
                    if (entry.getRightChild() != null && !entry.getRightChild().equals(preId)) {
                        addEntryToDeque(deque, entry.getRightChild());
                        preId = entry.getRightChild();
                    }
                }
//                sb.append(" | ");
                len = deque.size();
                System.out.println(sb);
                sb = new StringBuilder();
            }

            // print the internal page
            for (BTreeEntry entry : deque) {
                sb.append(entry.toString());
                sb.append(" ");
            }
            System.out.println(sb);
            sb = new StringBuilder();

            // print the leaf page
            boolean flag = true;
            while (!deque.isEmpty()) {
                BTreeEntry entry = deque.poll();
                if (flag) {
                    sb.append(getPageMaxMin(entry.getLeftChild()));
                    flag = false;
                }
                sb.append(getPageMaxMin(entry.getRightChild()));
            }
            System.out.println(sb);
        }
    }

    private void addEntryToDeque(Deque<BTreeEntry> deque, BTreePageId pageId) throws DbException, IOException, TransactionAbortedException {
        BTreeInternalPage internalPage = (BTreeInternalPage) Database.getBufferPool().getPage(pageId);
        Iterator<BTreeEntry> iterator = internalPage.iterator();
        while (iterator.hasNext()) {
            BTreeEntry entry = iterator.next();
            deque.offer(entry);
        }
    }


    /**
     * Handle the case when a B+ tree page becomes less than half full due to deletions.
     * If one of its siblings has extra tuples/entries, redistribute those tuples/entries.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the page which is less than half full
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #handleMinOccupancyLeafPage(TransactionId, Map, BTreeLeafPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
     * @see #handleMinOccupancyInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
     */
    private void handleMinOccupancyPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePage page)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId parentId = page.getParentId();
        BTreeEntry leftEntry = null;
        BTreeEntry rightEntry = null;
        BTreeInternalPage parent = null;

        // find the left and right siblings through the parent so we make sure they have
        // the same parent as the page. Find the entries in the parent corresponding to
        // the page and siblings
        if (parentId.pgcateg() != BTreePageId.ROOT_PTR) {
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
            Iterator<BTreeEntry> ite = parent.iterator();
            while (ite.hasNext()) {
                BTreeEntry e = ite.next();
                if (e.getLeftChild().equals(page.getId())) {
                    rightEntry = e;
                    break;
                } else if (e.getRightChild().equals(page.getId())) {
                    leftEntry = e;
                }
            }
        }

        if (page.getId().pgcateg() == BTreePageId.LEAF) {
            handleMinOccupancyLeafPage(tid, dirtypages, (BTreeLeafPage) page, parent, leftEntry, rightEntry);
        } else { // BTreePageId.INTERNAL
            handleMinOccupancyInternalPage(tid, dirtypages, (BTreeInternalPage) page, parent, leftEntry, rightEntry);
        }
    }

    /**
     * Handle the case when a leaf page becomes less than half full due to deletions.
     * If one of its siblings has extra tuples, redistribute those tuples.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the leaf page which is less than half full
     * @param parent     - the parent of the leaf page
     * @param leftEntry  - the entry in the parent pointing to the given page and its left-sibling
     * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #mergeLeafPages(TransactionId, Map, BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromLeafPage(BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry, boolean)
     */
    private void handleMinOccupancyLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page,
                                            BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        if (leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
        if (rightEntry != null) rightSiblingId = rightEntry.getRightChild();

        int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples() / 2; // ceiling
        if (leftSiblingId != null) {
            BTreeLeafPage leftSibling = (BTreeLeafPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (leftSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            } else {
                stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
            }
        } else if (rightSiblingId != null) {
            BTreeLeafPage rightSibling = (BTreeLeafPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (rightSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            } else {
                stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
            }
        }
    }

    /**
     * Steal tuples from a sibling and copy them to the given page so that both pages are at least
     * half full.  Update the parent's entry so that the key matches the key field of the first
     * tuple in the right-hand page.
     *
     * @param page           - the leaf page which is less than half full
     * @param sibling        - the sibling which has tuples to spare
     * @param parent         - the parent of the two leaf pages
     * @param entry          - the entry in the parent pointing to the two leaf pages
     * @param isRightSibling - whether the sibling is a right-sibling
     * @throws DbException
     */
    public void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling,
                                  BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling) throws DbException {
        // TODO: some code goes here
        //
        // Move some of the tuples from the sibling to the page so
        // that the tuples are evenly distributed. Be sure to update
        // the corresponding parent entry.
//        System.out.println("sibling capability: " + sibling.getMaxTuples());
//        System.out.println("Before steal, sibling:" + (sibling.getMaxTuples() - sibling.getNumEmptySlots()) + " ,page:" + (page.getMaxTuples() - page.getNumEmptySlots()));

        System.out.printf("BTreeFile: stealFromLeafPage %s -> %s isRight: %s\n", sibling.getId(), page.getId(), isRightSibling);

        Tuple steal = null;
        Iterator<Tuple> iterator;
//        if (sibling.getNumTuples()==sibling.getMaxTuples()/2 || sibling.getNumTuples()==sibling.getNumTuples()/2+1){
//
//        }

        // if right, get the min
        if (isRightSibling) {
            iterator = sibling.iterator();
        } else {
            iterator = sibling.reverseIterator();
        }

        int count = 0;
        int min = 0;
        int max = 0;
        while (sibling.getNumTuples() - page.getNumTuples() > 1) {
            if (iterator.hasNext()) {
                steal = iterator.next();
                if (count == 0) {
                    min = ((IntField) steal.getField(keyField)).getValue();
                }
            } else {
                throw new DbException("sibling does not have a entry");
            }
            sibling.deleteTuple(steal);
            page.insertTuple(steal);
            count++;
        }

        max = ((IntField) steal.getField(keyField)).getValue();

        // update the parent
        entry.setKey(steal.getField(keyField));
        System.out.printf("Move: min %d max %d pageNum %d siblingNum %d\n", min, max, page.getNumTuples(), sibling.getNumTuples());
        System.out.printf("PageInfo: min %s max %s\n", ((IntField) page.iterator().next().getField(keyField)).getValue(), ((IntField) page.reverseIterator().next().getField(keyField)).getValue());
//        System.out.println("After steal, sibling:" + (sibling.getMaxTuples() - sibling.getNumEmptySlots()) + " ,page:" + (page.getMaxTuples() - page.getNumEmptySlots()));

    }

    /**
     * Handle the case when an internal page becomes less than half full due to deletions.
     * If one of its siblings has extra entries, redistribute those entries.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the internal page which is less than half full
     * @param parent     - the parent of the internal page
     * @param leftEntry  - the entry in the parent pointing to the given page and its left-sibling
     * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #mergeInternalPages(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromLeftInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromRightInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     */
    private void handleMinOccupancyInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
                                                BTreeInternalPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        if (leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
        if (rightEntry != null) rightSiblingId = rightEntry.getRightChild();

        int maxEmptySlots = page.getMaxEntries() - page.getMaxEntries() / 2; // ceiling
        if (leftSiblingId != null) {
            BTreeInternalPage leftSibling = (BTreeInternalPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (leftSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            } else {
                stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
            }
        } else if (rightSiblingId != null) {
            BTreeInternalPage rightSibling = (BTreeInternalPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (rightSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            } else {
                stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
        }
    }

    /**
     * Steal entries from the left sibling and copy them to the given page so that both pages are at least
     * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
     * parent is "pulled down" to the right-hand page, and the last key in the left-hand page is "pushed up"
     * to the parent.  Update parent pointers as needed.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param page        - the internal page which is less than half full
     * @param leftSibling - the left sibling which has entries to spare
     * @param parent      - the parent of the two internal pages
     * @param parentEntry - the entry in the parent pointing to the two internal pages
     * @throws DbException
     * @throws TransactionAbortedException
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public void stealFromLeftInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
                                          BTreeInternalPage page, BTreeInternalPage leftSibling, BTreeInternalPage parent,
                                          BTreeEntry parentEntry) throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        // Move some of the entries from the left sibling to the page so
        // that the entries are evenly distributed. Be sure to update
        // the corresponding parent entry. Be sure to update the parent
        // pointers of all children in the entries that were moved.

        // move the page of leftSibling to page
        System.out.printf("BTreeFile: stealFromLeftInternalPage %s -> %s Num: leftSibling %d page %d\n", leftSibling.getId(), page.getId(),leftSibling.getNumEntries(),page.getNumEntries());

        Iterator<BTreeEntry> leftSiblingIterator;
        Iterator<BTreeEntry> pageIterator;
        BTreeEntry steal = null;
        BTreeEntry pageLeftEntry = null;
        BTreeEntry insert = null;
        BTreeEntry newParentEntry = null;

        while (leftSibling.getNumEntries() - page.getNumEntries() > 1) {
            leftSiblingIterator = leftSibling.reverseIterator();
            pageIterator = page.iterator();
            if (leftSiblingIterator.hasNext() && pageIterator.hasNext()) {
                steal = leftSiblingIterator.next();
                pageLeftEntry = pageIterator.next();
            } else {
                throw new DbException("leftSibling or page has no entry");
            }
            System.out.println("before parentEntry: " + parentEntry);
            leftSibling.deleteKeyAndRightChild(steal);
            Field key = parentEntry.getKey();
            parentEntry.setKey(steal.getKey());
            insert = new BTreeEntry(key, steal.getRightChild(), pageLeftEntry.getLeftChild());
            System.out.println("steal: " + steal + " insert: " + insert + " pageLeftEntry: " + pageLeftEntry);
            page.insertEntry(insert);
//            System.out.println("after parentEntry: " + parentEntry);
            // update parent

            // Be sure to update the parent pointers of all children in the entries that were moved.
            BTreePage p = (BTreePage) Database.getBufferPool().getPage(tid, steal.getRightChild(), Permissions.READ_ONLY);
            p.setParentId(page.getId());
            System.out.println("dirtyPages Put: " + p.getId());
            dirtypages.put(p.getId(), p);
        }
        System.out.printf("BTreeFile: stealEnd: leftSibling %d page %d\n" ,leftSibling.getNumEntries(),page.getNumEntries());

        // update parent
        newParentEntry = new BTreeEntry(steal.getKey(), leftSibling.pid, page.pid);
        parent.deleteKeyAndRightChild(parentEntry);
        parent.insertEntry(newParentEntry);

//        parentEntry.setKey(steal.getKey());
        System.out.println("After update parent: " + newParentEntry);

        // update dirtyPages
        dirtypages.put(page.getId(), page);
        dirtypages.put(parent.getId(), parent);
        dirtypages.put(leftSibling.getId(), leftSibling);
    }

    /**
     * Steal entries from the right sibling and copy them to the given page so that both pages are at least
     * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
     * parent is "pulled down" to the left-hand page, and the last key in the right-hand page is "pushed up"
     * to the parent.  Update parent pointers as needed.
     *
     * @param tid          - the transaction id
     * @param dirtypages   - the list of dirty pages which should be updated with all new dirty pages
     * @param page         - the internal page which is less than half full
     * @param rightSibling - the right sibling which has entries to spare
     * @param parent       - the parent of the two internal pages
     * @param parentEntry  - the entry in the parent pointing to the two internal pages
     * @throws DbException
     * @throws TransactionAbortedException
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public void stealFromRightInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
                                           BTreeInternalPage page, BTreeInternalPage rightSibling, BTreeInternalPage parent,
                                           BTreeEntry parentEntry) throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        // Move some of the entries from the right sibling to the page so
        // that the entries are evenly distributed. Be sure to update
        // the corresponding parent entry. Be sure to update the parent
        // pointers of all children in the entries that were moved.

        System.out.printf("BTreeFile: stealFromRightInternalPage %s -> %s\n", rightSibling.getId(), page.getId());
        // move the page of rightSibling to page
        Iterator<BTreeEntry> rightSiblingIterator;
        Iterator<BTreeEntry> pageIterator;
        BTreeEntry steal = null;
        BTreeEntry pageRightEntry = null;
        BTreeEntry insert = null;
        BTreeEntry newParentEntry = null;

        while (rightSibling.getNumEntries() - page.getNumEntries() > 1) {
            rightSiblingIterator = rightSibling.iterator();
            pageIterator = page.reverseIterator();
            if (rightSiblingIterator.hasNext() && pageIterator.hasNext()) {
                steal = rightSiblingIterator.next();
                pageRightEntry = pageIterator.next();
            } else {
                throw new DbException("leftSibling or page has no entry");
            }
            System.out.println("before parentEntry: " + parentEntry);
            rightSibling.deleteKeyAndLeftChild(steal);
            Field key = parentEntry.getKey();
            parentEntry.setKey(steal.getKey());
            insert = new BTreeEntry(key, pageRightEntry.getRightChild(), steal.getLeftChild());
//            insert = new BTreeEntry(key, steal.getLeftChild(), pageRightEntry.getRightChild());
            System.out.println("steal: " + steal + " insert: " + insert + " pageRightEntry: " + pageRightEntry);
            page.insertEntry(insert);
            // update parent

            // Be sure to update the parent pointers of all children in the entries that were moved.
            BTreePage p = (BTreePage) Database.getBufferPool().getPage(tid, steal.getLeftChild(), Permissions.READ_ONLY);
            p.setParentId(page.getId());
            System.out.println("dirtyPages Put: " + p.getId());
            dirtypages.put(p.getId(), p);
        }

        // update parent
        newParentEntry = new BTreeEntry(steal.getKey(), page.pid, rightSibling.pid);
        parent.deleteKeyAndRightChild(parentEntry);
        parent.insertEntry(newParentEntry);

//        parentEntry.setKey(steal.getKey());
        System.out.println("After parentEntry: " + newParentEntry);

        // update dirtyPages
        dirtypages.put(page.getId(), page);
        dirtypages.put(parent.getId(), parent);
        dirtypages.put(rightSibling.getId(), rightSibling);
    }

    /**
     * Merge two leaf pages by moving all tuples from the right page to the left page.
     * Delete the corresponding key and right child pointer from the parent, and recursively
     * handle the case when the parent gets below minimum occupancy.
     * Update sibling pointers as needed, and make the right page available for reuse.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage    - the left leaf page
     * @param rightPage   - the right leaf page
     * @param parent      - the parent of the two pages
     * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
     */
    public void mergeLeafPages(TransactionId tid, Map<PageId, Page> dirtypages,
                               BTreeLeafPage leftPage, BTreeLeafPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {
        System.out.printf("mergeLeafPage: %s %s -> %s\n", leftPage.getId().getPageNumber(), rightPage.getId().getPageNumber(), leftPage.getId().getPageNumber());
        // TODO: some code goes here
        //
        // Move all the tuples from the right page to the left page, update
        // the sibling pointers, and make the right page available for reuse.
        // Delete the entry in the parent corresponding to the two pages that are merging -
        // deleteParentEntry() will be useful here

        // Move all the tuples from the right page to the left page,
        Iterator<Tuple> iterator = rightPage.iterator();
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            rightPage.deleteTuple(tuple);
            leftPage.insertTuple(tuple);
        }

        // update the sibling pointers,
        leftPage.setRightSiblingId(rightPage.getRightSiblingId());
//        System.out.printf("page: %s set right is %s\n", leftPage.getId().getPageNumber(), rightPage.getRightSiblingId().getPageNumber());
        if (rightPage.getRightSiblingId() != null) {
            BTreeLeafPage page = (BTreeLeafPage) Database.getBufferPool().getPage(tid, rightPage.getRightSiblingId(), Permissions.READ_ONLY);
            page.setLeftSiblingId(leftPage.getId());
//            System.out.printf("page: %s set left is %s\n", page.getId().getPageNumber(), leftPage.getId().getPageNumber());
        }

        // make the right page available for reuse
        setEmptyPage(tid, dirtypages, rightPage.getId().getPageNumber());

        // Delete the entry in the parent corresponding to the two pages that are merging
        deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

        // update dirtyPages
        dirtypages.put(leftPage.getId(), leftPage);
        dirtypages.put(rightPage.getId(), rightPage);
        dirtypages.put(parent.getId(), parent);

        System.out.printf("BTreeFile: merge end, now %s page size is %s\n", leftPage.getId().getPageNumber(), leftPage.getNumTuples());
    }

    /**
     * Merge two internal pages by moving all entries from the right page to the left page
     * and "pulling down" the corresponding key from the parent entry.
     * Delete the corresponding key and right child pointer from the parent, and recursively
     * handle the case when the parent gets below minimum occupancy.
     * Update parent pointers as needed, and make the right page available for reuse.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage    - the left internal page
     * @param rightPage   - the right internal page
     * @param parent      - the parent of the two pages
     * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public void mergeInternalPages(TransactionId tid, Map<PageId, Page> dirtypages,
                                   BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {
        System.out.printf("mergeInternalPage: %s %s -> %s \n", leftPage.getId().getPageNumber(), rightPage.getId().getPageNumber(), leftPage.getId().getPageNumber());
        // TODO: some code goes here
        //
        // Move all the entries from the right page to the left page, update
        // the parent pointers of the children in the entries that were moved,
        // and make the right page available for reuse
        // Delete the entry in the parent corresponding to the two pages that are merging -
        // deleteParentEntry() will be useful here

        // Move all the entries from the right page to the left page
        Iterator<BTreeEntry> iterator = rightPage.iterator();
        BTreeEntry rightEntry = leftPage.reverseIterator().next();
        BTreeEntry leftEntry = rightPage.iterator().next();
        BTreeEntry entry = null;

        BTreeEntry insert = new BTreeEntry(parentEntry.getKey(), rightEntry.getRightChild(), leftEntry.getLeftChild());
        leftPage.insertEntry(insert);
        while (iterator.hasNext()) {
            entry = iterator.next();
            rightPage.deleteKeyAndRightChild(entry);
            leftPage.insertEntry(entry);

            BTreePage page = (BTreePage) Database.getBufferPool().getPage(tid, entry.getLeftChild(), Permissions.READ_ONLY);
            page.setParentId(leftPage.getId());
            dirtypages.put(page.getId(), page);
        }
        if (entry != null && entry.getRightChild() != null) {
            BTreePage page = (BTreePage) Database.getBufferPool().getPage(tid, entry.getRightChild(), Permissions.READ_ONLY);
            page.setParentId(leftPage.getId());
            dirtypages.put(page.getId(), page);
        }

        // update the parent pointers of the children in the entries that were moved and make the right page available for reuse
        setEmptyPage(tid, dirtypages, rightPage.getId().getPageNumber());

        // Delete the entry in the parent corresponding to the two pages that are merging
        deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

        // update dirtyPages
        dirtypages.put(leftPage.getId(), leftPage);
        dirtypages.put(rightPage.getId(), rightPage);
        dirtypages.put(parent.getId(), parent);
    }

    /**
     * Method to encapsulate the process of deleting an entry (specifically the key and right child)
     * from a parent node.  If the parent becomes empty (no keys remaining), that indicates that it
     * was the root node and should be replaced by its one remaining child.  Otherwise, if it gets
     * below minimum occupancy for non-root internal nodes, it should steal from one of its siblings or
     * merge with a sibling.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage    - the child remaining after the key and right child are deleted
     * @param parent      - the parent containing the entry to be deleted
     * @param parentEntry - the entry to be deleted
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
     */
    private void deleteParentEntry(TransactionId tid, Map<PageId, Page> dirtypages,
                                   BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {

        // delete the entry in the parent.  If
        // the parent is below minimum occupancy, get some tuples from its siblings
        // or merge with one of the siblings
        parent.deleteKeyAndRightChild(parentEntry);
        int maxEmptySlots = parent.getMaxEntries() - parent.getMaxEntries() / 2; // ceiling
        if (parent.getNumEmptySlots() == parent.getMaxEntries()) {
            // This was the last entry in the parent.
            // In this case, the parent (root node) should be deleted, and the merged
            // page will become the new root
            BTreePageId rootPtrId = parent.getParentId();
            if (rootPtrId.pgcateg() != BTreePageId.ROOT_PTR) {
                throw new DbException("attempting to delete a non-root node");
            }
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, rootPtrId, Permissions.READ_WRITE);
            leftPage.setParentId(rootPtrId);
            rootPtr.setRootId(leftPage.getId());

            // release the parent page for reuse
            setEmptyPage(tid, dirtypages, parent.getId().getPageNumber());
        } else if (parent.getNumEmptySlots() > maxEmptySlots) {
            handleMinOccupancyPage(tid, dirtypages, parent);
        }
    }

    /**
     * Delete a tuple from this BTreeFile.
     * May cause pages to merge or redistribute entries/tuples if the pages
     * become less than half full.
     *
     * @param tid - the transaction id
     * @param t   - the tuple to delete
     * @return a list of all pages that were dirtied by this operation. Could include
     * many pages since parent pointers will need to be updated when an internal node merges.
     * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
     */
    public List<Page> deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        Map<PageId, Page> dirtypages = new HashMap<>();

        BTreePageId pageId = new BTreePageId(tableid, t.getRecordId().getPageId().getPageNumber(),
                BTreePageId.LEAF);
        BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);

        // if the page is below minimum occupancy, get some tuples from its siblings
        // or merge with one of the siblings
        int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples() / 2; // ceiling
        if (page.getNumEmptySlots() > maxEmptySlots) {
            handleMinOccupancyPage(tid, dirtypages, page);
        }

        return new ArrayList<>(dirtypages.values());
    }

    /**
     * Get a read lock on the root pointer page. Create the root pointer page and root page
     * if necessary.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @return the root pointer page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    BTreeRootPtrPage getRootPtrPage(TransactionId tid, Map<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
        synchronized (this) {
            if (f.length() == 0) {
                // create the root pointer page and the root page
                BufferedOutputStream bw = new BufferedOutputStream(
                        new FileOutputStream(f, true));
                byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
                byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
                bw.write(emptyRootPtrData);
                bw.write(emptyLeafData);
                bw.close();
            }
        }

        // get a read lock on the root pointer page
        return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
    }

    /**
     * Get the page number of the first empty page in this BTreeFile.
     * Creates a new page if none of the existing pages are empty.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @return the page number of the first empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public int getEmptyPageNo(TransactionId tid, Map<PageId, Page> dirtypages)
            throws DbException, IOException, TransactionAbortedException {
        // get a read lock on the root pointer page and use it to locate the first header page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        int emptyPageNo = 0;

        if (headerId != null) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            int headerPageCount = 0;
            // try to find a header page with an empty slot
            while (headerPage != null && headerPage.getEmptySlot() == -1) {
                headerId = headerPage.getNextPageId();
                if (headerId != null) {
                    headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
                    headerPageCount++;
                } else {
                    headerPage = null;
                }
            }

            // if headerPage is not null, it must have an empty slot
            if (headerPage != null) {
                headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
                int emptySlot = headerPage.getEmptySlot();
                headerPage.markSlotUsed(emptySlot, true);
                emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
            }
        }

        // at this point if headerId is null, either there are no header pages
        // or there are no free slots
        if (headerId == null) {
            synchronized (this) {
                // create the new page
                BufferedOutputStream bw = new BufferedOutputStream(
                        new FileOutputStream(f, true));
                byte[] emptyData = BTreeInternalPage.createEmptyPageData();
                bw.write(emptyData);
                bw.close();
                emptyPageNo = numPages();
            }
        }

        return emptyPageNo;
    }

    /**
     * Method to encapsulate the process of creating a new page.  It reuses old pages if possible,
     * and creates a new page if none are available.  It wipes the page on disk and in the cache and
     * returns a clean copy locked with read-write permission
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pgcateg    - the BTreePageId category of the new page.  Either LEAF, INTERNAL, or HEADER
     * @return the new empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getEmptyPageNo(TransactionId, Map)
     * @see #setEmptyPage(TransactionId, Map, int)
     */
    private Page getEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int pgcateg)
            throws DbException, IOException, TransactionAbortedException {
        // create the new page
        int emptyPageNo = getEmptyPageNo(tid, dirtypages);
        BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pgcateg);

        // write empty page to disk
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(BTreeRootPtrPage.getPageSize() + (long) (emptyPageNo - 1) * BufferPool.getPageSize());
        rf.write(BTreePage.createEmptyPageData());
        rf.close();

        // make sure the page is not in the buffer pool	or in the local cache
        Database.getBufferPool().removePage(newPageId);
        dirtypages.remove(newPageId);

        return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
    }

    /**
     * Mark a page in this BTreeFile as empty. Find the corresponding header page
     * (create it if needed), and mark the corresponding slot in the header page as empty.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param emptyPageNo - the page number of the empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getEmptyPage(TransactionId, Map, int)
     */
    public void setEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int emptyPageNo)
            throws DbException, IOException, TransactionAbortedException {

        // if this is the last page in the file (and not the only page), just
        // truncate the file
        // @TODO: Commented out because we should probably do this somewhere else in case the transaction aborts....
        //		synchronized(this) {
        //			if(emptyPageNo == numPages()) {
        //				if(emptyPageNo <= 1) {
        //					// if this is the only page in the file, just return.
        //					// It just means we have an empty root page
        //					return;
        //				}
        //				long newSize = f.length() - BufferPool.getPageSize();
        //				FileOutputStream fos = new FileOutputStream(f, true);
        //				FileChannel fc = fos.getChannel();
        //				fc.truncate(newSize);
        //				fc.close();
        //				fos.close();
        //				return;
        //			}
        //		}

        // otherwise, get a read lock on the root pointer page and use it to locate
        // the first header page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        BTreePageId prevId = null;
        int headerPageCount = 0;

        // if there are no header pages, create the first header page and update
        // the header pointer in the BTreeRootPtrPage
        if (headerId == null) {
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);

            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            rootPtr.setHeaderId(headerId);
        }

        // iterate through all the existing header pages to find the one containing the slot
        // corresponding to emptyPageNo
        while (headerId != null && (headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            prevId = headerId;
            headerId = headerPage.getNextPageId();
            headerPageCount++;
        }

        // at this point headerId should either be null or set with
        // the headerPage containing the slot corresponding to emptyPageNo.
        // Add header pages until we have one with a slot corresponding to emptyPageNo
        while ((headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
            BTreeHeaderPage prevPage = (BTreeHeaderPage) getPage(tid, dirtypages, prevId, Permissions.READ_WRITE);

            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            headerPage.setPrevPageId(prevId);
            prevPage.setNextPageId(headerId);

            headerPageCount++;
            prevId = headerId;
        }

        // now headerId should be set with the headerPage containing the slot corresponding to
        // emptyPageNo
        BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
        int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage.getNumSlots();
        headerPage.markSlotUsed(emptySlot, false);
    }

    /**
     * get the specified tuples from the file based on its IndexPredicate value on
     * behalf of the specified transaction. This method will acquire a read lock on
     * the affected pages of the file, and may block until the lock can be
     * acquired.
     *
     * @param tid   - the transaction id
     * @param ipred - the index predicate value to filter on
     * @return an iterator for the filtered tuples
     */
    public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
        return new BTreeSearchIterator(this, tid, ipred);
    }

    /**
     * Get an iterator for all tuples in this B+ tree file in sorted order. This method
     * will acquire a read lock on the affected pages of the file, and may block until
     * the lock can be acquired.
     *
     * @param tid - the transaction id
     * @return an iterator for all the tuples in this file
     */
    public DbFileIterator iterator(TransactionId tid) {
        return new BTreeFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a BTreeFile
 */
class BTreeFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    final TransactionId tid;
    final BTreeFile f;

    /**
     * Constructor for this iterator
     *
     * @param f   - the BTreeFile containing the tuples
     * @param tid - the transaction id
     */
    public BTreeFileIterator(BTreeFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    /**
     * Open this iterator by getting an iterator on the first leaf page
     */
    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
                tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        curp = f.findLeafPage(tid, root, null);
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page by following the right sibling pointer.
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && curp != null) {
            BTreePageId nextp = curp.getRightSiblingId();
            if (nextp == null) {
                curp = null;
            } else {
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
                        nextp, Permissions.READ_ONLY);
                it = curp.iterator();
                if (!it.hasNext())
                    it = null;
            }
        }

        if (it == null)
            return null;
        return it.next();
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
        curp = null;
    }
}

/**
 * Helper class that implements the DbFileIterator for search tuples on a
 * B+ Tree File
 */
class BTreeSearchIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    final TransactionId tid;
    final BTreeFile f;
    final IndexPredicate ipred;

    /**
     * Constructor for this iterator
     *
     * @param f     - the BTreeFile containing the tuples
     * @param tid   - the transaction id
     * @param ipred - the predicate to filter on
     */
    public BTreeSearchIterator(BTreeFile f, TransactionId tid, IndexPredicate ipred) {
        this.f = f;
        this.tid = tid;
        this.ipred = ipred;
    }

    /**
     * Open this iterator by getting an iterator on the first leaf page applicable
     * for the given predicate operation
     */
    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
                tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        if (ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN
                || ipred.getOp() == Op.GREATER_THAN_OR_EQ) {
            System.out.println("open: find " + ipred.getField());
            curp = f.findLeafPage(tid, root, ipred.getField());
        } else {
            curp = f.findLeafPage(tid, root, null);
        }
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples matching
     * the predicate or from the next page by following the right sibling pointer.
     *
     * @return the next tuple matching the predicate, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException,
            NoSuchElementException {
        while (it != null) {

            while (it.hasNext()) {
                Tuple t = it.next();
                if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
                    System.out.println("BTreeSearch: read: " + ((IntField) t.getField(f.keyField())).getValue() + " , page: " + curp.pid);
                    return t;
                } else if (ipred.getOp() == Op.LESS_THAN || ipred.getOp() == Op.LESS_THAN_OR_EQ) {
                    // if the predicate was not satisfied and the operation is less than, we have
                    // hit the end
                    return null;
                } else if (ipred.getOp() == Op.EQUALS &&
                        t.getField(f.keyField()).compare(Op.GREATER_THAN, ipred.getField())) {
                    // if the tuple is now greater than the field passed in and the operation
                    // is equals, we have reached the end
                    return null;
                }
            }

            BTreePageId nextp = curp.getRightSiblingId();
            // if there are no more pages to the right, end the iteration
            if (nextp == null) {
                return null;
            } else {
                System.out.println("BTreeSearch: readPage: " + nextp);
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
                        nextp, Permissions.READ_ONLY);
                it = curp.iterator();
            }
        }

        return null;
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
    }
}
