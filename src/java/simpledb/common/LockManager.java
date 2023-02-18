package simpledb.common;

import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.storage.RecordId;
import simpledb.storage.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {

    static private Map<RecordId, ReadWriteLock> tupleRWLockMap = new ConcurrentHashMap<>();
    static private Map<RecordId, ReentrantLock> tupleLockMap = new ConcurrentHashMap<>();
    static private Map<PageId, ReadWriteLock> pageRWLockMap = new ConcurrentHashMap<>();
    static private Map<Integer, ReentrantLock> tableLockMap = new ConcurrentHashMap<>();

    public LockManager() {
    }

    static public void addLockByTuple(RecordId rid) {
        tupleLockMap.put(rid, new ReentrantLock());
    }

    static public void lockByTuple(RecordId rid) {
        tupleLockMap.get(rid).lock();
    }

    static public void unlockByTuple(RecordId rid) {
        tupleLockMap.get(rid).unlock();
    }

    static public void addRWLockByTuple(RecordId rid) {
        tupleRWLockMap.put(rid, new ReentrantReadWriteLock());
    }

    static public void RLockByTuple(RecordId rid) {
        tupleRWLockMap.get(rid).readLock().lock();
    }

    static public void unRLockByTuple(RecordId rid) {
        tupleRWLockMap.get(rid).readLock().unlock();
    }

    static public void WLockByTuple(RecordId rid) {
        tupleRWLockMap.get(rid).writeLock().lock();
    }

    static public void unWLockByTuple(RecordId rid) {
        tupleRWLockMap.get(rid).writeLock().unlock();
    }

    static public void addRWLockByPage(PageId pid) {
        pageRWLockMap.put(pid, new ReentrantReadWriteLock());
    }

    static public void RLockByPage(PageId pid) {
        pageRWLockMap.get(pid).readLock().lock();
    }

    static public void unRLockByPage(PageId pid) {
        pageRWLockMap.get(pid).readLock().unlock();
    }

    static public void WLockByPage(PageId pid) {
        pageRWLockMap.get(pid).writeLock().lock();
    }

    static public void unWLockByPage(PageId pid) {
        pageRWLockMap.get(pid).writeLock().unlock();
    }

    static public void addLockByTable(Integer tid) {
        tableLockMap.put(tid, new ReentrantLock());
    }

    static public void lockByTable(Integer tid) {
        tableLockMap.get(tid).lock();
    }

    static public void unlockByTable(Integer tid) {
        tableLockMap.get(tid).unlock();
    }
}
