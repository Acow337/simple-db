package simpledb.common;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// refer to bustub https://github.com/cmu-db/bustub
// page-level lock
public class LockManager {
    enum LockMode {
        SHARED, EXCLUSIVE;
    }

    class LockRequest {
        public TransactionId tid;
        public LockMode lockMode;
        public boolean granted = false;

        public LockRequest(TransactionId tid, LockMode lockMode, PageId pageId) {
            this.tid = tid;
            this.lockMode = lockMode;
        }
    }

    class LockRequestQueue {
        public Queue<LockRequest> queue;
        // coordination
        public Lock latch;
        public Condition condition;
        public TransactionId upgradingTid;

        public LockRequestQueue() {
            queue = new ConcurrentLinkedQueue<>();
            latch = new ReentrantLock();
            condition = latch.newCondition();
        }

        public boolean offer(LockRequest request) {
            return queue.offer(request);
        }

        public LockRequest poll() {
            return queue.poll();
        }
    }

    private Map<PageId, LockRequestQueue> pageLockMap;
    private Map<Integer, List<Integer>> waitsForMap;
    private Map<TransactionId, Set<PageId>> txnMarkMap;
    private Thread cycleDetectionThread;
    private volatile boolean enableCycleDetection;

    public LockManager() {
        enableCycleDetection = true;
        pageLockMap = new ConcurrentHashMap<>();
        waitsForMap = new ConcurrentHashMap<>();
        txnMarkMap = new ConcurrentHashMap<>();
        //TODO
        cycleDetectionThread = new Thread();
    }


    public void lockPage(TransactionId tid, PageId pageId, Permissions perm) throws DeadlockException {
        System.out.println("lockPage tid: " + tid + " pageId: " + pageId + " perm: " + perm.toString());
        if (!pageLockMap.containsKey(pageId)) pageLockMap.put(pageId, new LockRequestQueue());
        if (!txnMarkMap.containsKey(tid)) txnMarkMap.put(tid, new HashSet<>());
        txnMarkMap.get(tid).add(pageId);
        LockMode mode = null;
        LockMode formerMode = null;
        LockRequest formerRequest = null;
        int lockNum = 0;
        boolean isOtherHasLock = false;
        if (perm == Permissions.READ_ONLY) {
            mode = LockMode.SHARED;
        } else if (perm == Permissions.READ_WRITE) {
            mode = LockMode.EXCLUSIVE;
        }

        LockRequest newRequest = new LockRequest(tid, mode, pageId);
        LockRequestQueue requestQueue = pageLockMap.get(pageId);

        // if no lock on the page, lock literally
        if (requestQueue.queue.isEmpty()) {
            newRequest.granted = true;
            System.out.println("pageId: " + pageId + " queue size: " + requestQueue.queue.size());
            System.out.println(tid + " requestQueue is empty, get directly");
            requestQueue.offer(newRequest);
            return;
        }

        // check if the txn already has lock
        for (LockRequest request : requestQueue.queue) {
            lockNum++;
            if (request.tid == tid) {
                formerRequest = request;
            } else {
                isOtherHasLock = true;
            }
        }

        //TODO if the txn already has lock on the page
        if (formerRequest != null) {
            // the txn already has lock
            if (formerMode == LockMode.SHARED) {
                if (mode == LockMode.SHARED) {
                    return;
                } else if (mode == LockMode.EXCLUSIVE) {
                    if (isOtherHasLock) {
                        // throw exception because deadlock may happen (upgrade failure)
                        throw new DeadlockException();
                    } else {
                        // upgrade the lock
                        formerRequest.lockMode = LockMode.EXCLUSIVE;
                        return;
                    }
                }
            } else if (formerMode == LockMode.EXCLUSIVE) {
                return;
            }
        } else {
            if (lockNum > 1) {
                if (newRequest.lockMode == LockMode.SHARED) {
                    requestQueue.offer(newRequest);
                    return;
                } else {
                    requestQueue.latch.lock();
                    requestQueue.condition.awaitUninterruptibly();
                    requestQueue.latch.unlock();
                    lockPage(tid, pageId, perm);
                }
            } else if (lockNum == 1) {
                LockRequest other = requestQueue.queue.peek();
                if (other.lockMode == LockMode.EXCLUSIVE) {
                    System.out.println(tid + " the other lock is exclusive, wait some time");
                    requestQueue.latch.lock();
                    requestQueue.condition.awaitUninterruptibly();
                    requestQueue.latch.unlock();
                    lockPage(tid, pageId, perm);
                } else if (other.lockMode == LockMode.SHARED) {
                    if (mode == LockMode.SHARED) {
                        requestQueue.offer(newRequest);
                        return;
                    } else if (mode == LockMode.EXCLUSIVE) {
                        requestQueue.latch.lock();
                        requestQueue.condition.awaitUninterruptibly();
                        requestQueue.latch.unlock();
                        lockPage(tid, pageId, perm);
                    }
                }
            }
        }
    }

    public void unLockPage(TransactionId tid, PageId pageId) {
        System.out.println("unLockPage tid: " + tid + " pageId: " + pageId);
        LockRequestQueue requestQueue = pageLockMap.get(pageId);
        LockRequest removeRequest = null;
        for (LockRequest request : requestQueue.queue) {
            if (tid == request.tid) {
                removeRequest = request;
            }
        }
        requestQueue.queue.remove(removeRequest);
        requestQueue.latch.lock();
        requestQueue.condition.signalAll();
        requestQueue.latch.unlock();
    }

    public Set<PageId> getMarkPages(TransactionId tid) {
        return txnMarkMap.get(tid);
    }

    public void removeTxnMark(TransactionId tid) {
        txnMarkMap.remove(tid);
    }

}
