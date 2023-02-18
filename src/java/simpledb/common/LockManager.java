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

    static void addLockByTuple(RecordId rid) {
        tupleLockMap.put(rid, new ReentrantLock());
    }

    static void lockByTuple(RecordId rid) {
        tupleLockMap.get(rid).lock();
    }

    static void unlockByTuple(RecordId rid) {
        tupleLockMap.get(rid).unlock();
    }

    static void addRWLockByTuple(RecordId rid) {
        tupleRWLockMap.put(rid, new ReentrantReadWriteLock());
    }

    static void RLockByTuple(RecordId rid) {
        tupleRWLockMap.get(rid).readLock().lock();
    }

    static void unRLockByTuple(RecordId rid) {
        tupleRWLockMap.get(rid).readLock().unlock();
    }

    static void WLockByTuple(RecordId rid) {
        tupleRWLockMap.get(rid).writeLock().lock();
    }

    static void unWLockByTuple(RecordId rid) {
        tupleRWLockMap.get(rid).writeLock().unlock();
    }

    static void addRWLockByPage(PageId pid) {
        pageRWLockMap.put(pid, new ReentrantReadWriteLock());
    }

    static void RLockByPage(PageId pid) {
        pageRWLockMap.get(pid).readLock().lock();
    }

    static void unRLockByPage(PageId pid) {
        pageRWLockMap.get(pid).readLock().unlock();
    }

    static void WLockByPage(PageId pid) {
        pageRWLockMap.get(pid).writeLock().lock();
    }

    static void unWLockByPage(PageId pid) {
        pageRWLockMap.get(pid).writeLock().unlock();
    }

    static void addLockByTable(Integer tid) {
        tableLockMap.put(tid, new ReentrantLock());
    }

    static void lockByTable(Integer tid) {
        tableLockMap.get(tid).lock();
    }

    static void unlockByTable(Integer tid) {
        tableLockMap.get(tid).unlock();
    }
}
