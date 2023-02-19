package simpledb.common;

import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.storage.RecordId;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {

    //    static private Map<RecordId, ReadWriteLock> tupleRWLockMap = new ConcurrentHashMap<>();
//    static private Map<RecordId, ReentrantLock> tupleLockMap = new ConcurrentHashMap<>();
    static private Map<PageId, ReadWriteLock> pageRWLockMap = new ConcurrentHashMap<>();
    //    static private Map<Integer, ReentrantLock> tableLockMap = new ConcurrentHashMap<>();
    static private Map<TransactionId, Set<PageId>> txnTrackMap = new ConcurrentHashMap<>();

    public LockManager() {
    }

//    static public void addLockByTuple(RecordId rid) {
//        if (!tupleLockMap.containsKey(rid)) tupleLockMap.put(rid, new ReentrantLock());
//    }
//
//    static public void lockByTuple(RecordId rid) {
//        tupleLockMap.get(rid).lock();
//    }
//
//    static public void unlockByTuple(RecordId rid) {
//        tupleLockMap.get(rid).unlock();
//    }
//
//    static public void addRWLockByTuple(RecordId rid) {
//        if (!tupleRWLockMap.containsKey(rid)) tupleRWLockMap.put(rid, new ReentrantReadWriteLock());
//    }
//
//    static public void RLockByTuple(RecordId rid) {
//        tupleRWLockMap.get(rid).readLock().lock();
//    }
//
//    static public void unRLockByTuple(RecordId rid) {
//        tupleRWLockMap.get(rid).readLock().unlock();
//    }
//
//    static public void WLockByTuple(RecordId rid) {
//        tupleRWLockMap.get(rid).writeLock().lock();
//    }
//
//    static public void unWLockByTuple(RecordId rid) {
//        tupleRWLockMap.get(rid).writeLock().unlock();
//    }

    static public void addRWLockByPage(TransactionId tid, PageId pid) {
        if (!pageRWLockMap.containsKey(pid)) {
            pageRWLockMap.put(pid, new ReentrantReadWriteLock());
        }
    }

    static public void RLockByPage(TransactionId tid, PageId pid) {
        pageRWLockMap.get(pid).readLock().lock();
    }

    static public void unRLockByPage(TransactionId tid, PageId pid) {
        pageRWLockMap.get(pid).readLock().unlock();
    }

    static public void WLockByPage(TransactionId tid, PageId pid) {
        pageRWLockMap.get(pid).writeLock().lock();
    }

    static public void unWLockByPage(TransactionId tid, PageId pid) {
        pageRWLockMap.get(pid).writeLock().unlock();
    }

//    static public void addLockByTable(Integer tid) {
//        if (!tableLockMap.containsKey(tid)) tableLockMap.put(tid, new ReentrantLock());
//    }
//
//    static public void lockByTable(Integer tid) {
//        tableLockMap.get(tid).lock();
//    }
//
//    static public void unlockByTable(Integer tid) {
//        tableLockMap.get(tid).unlock();
//    }
}
