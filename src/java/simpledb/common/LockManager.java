package simpledb.common;

import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
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
    private Map<Long, List<Long>> waitsForMap;
    private Map<TransactionId, Set<PageId>> txnMarkMap;


    public LockManager() {
        pageLockMap = new ConcurrentHashMap<>();
        waitsForMap = new ConcurrentHashMap<>();
        txnMarkMap = new ConcurrentHashMap<>();
    }

    public void acquireLock(TransactionId tid, PageId pageId, Permissions perm) throws DeadlockException, TransactionAbortedException {
        long begin = System.currentTimeMillis();
        while (!lockPage(tid, pageId, perm, 0)) {
            System.out.println("wait");
            if (System.currentTimeMillis() - begin > 500)
                throw new TransactionAbortedException();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
        }
        System.out.println("get Lock: tid: " + tid + " pageId: " + pageId + " perm: " + perm);
    }

    private synchronized boolean lockPage(TransactionId tid, PageId pageId, Permissions perm, int retry) throws DeadlockException {
        System.out.println("lockPage tid: " + tid + " pageId: " + pageId + " perm: " + perm.toString() + " retry: " + retry);
        if (retry > 3) {
            return false;
        }
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
            return true;
        }

        // check if the txn already has lock
        for (LockRequest request : requestQueue.queue) {
            lockNum++;
            if (request.tid == tid) {
                formerRequest = request;
                formerMode = request.lockMode;
            } else {
                isOtherHasLock = true;
            }
        }

        System.out.println("requestQueue is: " + requestQueue.queue.size() + " formerRequest: " + (formerRequest != null) + " formerMode: " + formerMode + " mode:" + mode);

        // if the txn already has lock on the page
        if (formerRequest != null) {
            // the txn already has lock
            if (formerMode == LockMode.SHARED) {
                if (mode == LockMode.SHARED) {

                } else if (mode == LockMode.EXCLUSIVE) {
                    if (isOtherHasLock) {
                        // when 2 Txn race, one should abort, the other should wait and retry
                        System.out.println("LockManager:   upgrade failure   tid: " + tid);
                        return lockPage(tid, pageId, perm, retry + 1);
                    } else {
                        // upgrade the lock
                        System.out.println("LockManager:   upgrade success   ");
                        formerRequest.lockMode = LockMode.EXCLUSIVE;
                    }
                }
            } else if (formerMode == LockMode.EXCLUSIVE) {

            }
        } else {
            if (lockNum > 1) {
                if (newRequest.lockMode == LockMode.SHARED) {
                    requestQueue.offer(newRequest);
                } else {
                    for (LockRequest request : requestQueue.queue) {
                        putWaitForMap(tid.getId(), request.tid.getId());
                    }
                    return lockPage(tid, pageId, perm, retry + 1);
                }
            } else if (lockNum == 1) {
                LockRequest other = requestQueue.queue.peek();
                if (other.lockMode == LockMode.EXCLUSIVE) {
                    System.out.println(tid + " the other lock is exclusive, wait some time");
                    putWaitForMap(tid.getId(), other.tid.getId());
                    return lockPage(tid, pageId, perm, retry + 1);
                } else if (other.lockMode == LockMode.SHARED) {
                    if (mode == LockMode.SHARED) {
                        requestQueue.offer(newRequest);
                    } else if (mode == LockMode.EXCLUSIVE) {
                        System.out.println(tid + " the other lock is exclusive, wait some time");
                        putWaitForMap(tid.getId(), other.tid.getId());
                        return lockPage(tid, pageId, perm, retry + 1);
                    }
                }
            }
        }
        return true;
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

    public void putWaitForMap(Long k, Long v) throws DeadlockException {
        if (!waitsForMap.containsKey(k)) waitsForMap.put(k, new LinkedList<>());
        waitsForMap.get(k).add(v);
        if (isCycle(k))
            throw new DeadlockException();
    }

    public boolean isCycle(Long start) {
        Deque<Long> queue = new LinkedList<>();
        List<Long> list = waitsForMap.get(start);
        for (Long i : list) {
            queue.offer(i);
        }
        while (!queue.isEmpty()) {
            Long i = queue.poll();
            if (i == start) return true;
            List<Long> l = waitsForMap.get(i);
            if (l != null) {
                for (Long item : l) {
                    queue.offer(item);
                }
            }
        }
        return false;
    }


}
