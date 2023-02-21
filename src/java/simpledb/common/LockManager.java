package simpledb.common;

import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.storage.RecordId;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
//    // 1. every txn can only get readLock one time
//    // 2.
//
//    static private Map<PageId, ReadWriteLock> pageRWLockMap = new ConcurrentHashMap<>();
//    static private Map<TransactionId, Set<PageId>> txnRTrackMap = new ConcurrentHashMap<>();
//    static private Map<TransactionId, Set<PageId>> txnWTrackMap = new ConcurrentHashMap<>();
//
//    public LockManager() {
//    }
//
//    // every time when get the page, we should make sure they already have reentrantLock to use
//    static public void addRWLockByPage(TransactionId tid, PageId pid) {
//        if (!txnRTrackMap.containsKey(tid)) {
//            System.out.println("add txnRTrackMap success tid: " + tid + " pid: " + pid);
//            txnRTrackMap.put(tid, new HashSet<>());
//        }
//        if (!txnWTrackMap.containsKey(tid)) {
//            System.out.println("add txnWTrackMap success tid: " + tid + " pid: " + pid);
//            txnWTrackMap.put(tid, new HashSet<>());
//        }
//        if (!pageRWLockMap.containsKey(pid)) {
//            System.out.println("add pageRWLockMap success tid: " + tid + " pid: " + pid);
//            pageRWLockMap.put(pid, new ReentrantReadWriteLock());
//        }
//    }
//
//    static public void RLockByPage(TransactionId tid, PageId pid) {
//        if (txnRTrackMap.get(tid).contains(pid)) return;
//        if (txnWTrackMap.get(tid) != null && txnWTrackMap.get(tid).contains(pid)) return;
//        txnRTrackMap.get(tid).add(pid);
//        System.out.println("RLockByPage " + " tid: " + tid + " pid: " + pid);
//        pageRWLockMap.get(pid).readLock().lock();
//    }
//
//    static public void unRLockByPage(TransactionId tid, PageId pid) {
//        if (txnRTrackMap.get(tid).contains(pid)) {
//            txnRTrackMap.get(tid).remove(pid);
//            System.out.println("unRLockByPage " + "tid: " + tid);
//            pageRWLockMap.get(pid).readLock().unlock();
//        } else {
//            System.out.println("unRLockByPage fail");
//        }
//    }
//
//    // if the txn already has the readLock, upgrade the lock to writeLock
//    static public void WLockByPage(TransactionId tid, PageId pid) {
//        if (txnWTrackMap.get(tid).contains(pid)) return;
//        if (txnRTrackMap.get(tid).contains(pid)) {
//            System.out.println("=======remove=======");
//            txnRTrackMap.get(tid).remove(pid);
//            System.out.println("unRLockByPage " + " tid: " + tid + " pid: " + pid);
////            pageRWLockMap.get(pid).readLock().unlock();
//        }
//        txnWTrackMap.get(tid).add(pid);
//        System.out.println("WLockByPage " + " tid: " + tid + " pid: " + pid);
//        pageRWLockMap.get(pid).writeLock().lock();
//    }
//
//    static public void unWLockByPage(TransactionId tid, PageId pid) {
//        if (txnWTrackMap.get(tid).contains(pid)) {
//            txnWTrackMap.get(tid).remove(pid);
//            pageRWLockMap.get(pid).writeLock().unlock();
//        } else {
//            System.out.println("unWLockByPage fail");
//        }
//    }
//
//    static public void unRWLockByPage(TransactionId tid, PageId pid) {
//        unRLockByPage(tid, pid);
//        unWLockByPage(tid, pid);
//    }

}
