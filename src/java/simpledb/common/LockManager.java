package simpledb.common;

import simpledb.storage.RecordId;
import simpledb.storage.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {

    static private Map<RecordId, ReadWriteLock> tupleRWLockMap = new ConcurrentHashMap<>();
    static private Map<RecordId, ReentrantLock> tupleLockMap = new ConcurrentHashMap<>();
    static private Map<Integer, ReentrantLock> tableLockMap = new ConcurrentHashMap<>();

    public LockManager() {
    }

    static void lockByTuple(RecordId rid) {
        tupleLockMap.get(rid).lock();
    }

    static void unlockByTuple(RecordId rid) {
        tupleLockMap.get(rid).unlock();
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

    static void lockByTable(Integer tid) {
        tableLockMap.get(tid).lock();
    }

    static void unlockByTable(Integer tid) {
        tableLockMap.get(tid).unlock();
    }
}
