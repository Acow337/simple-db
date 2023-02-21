package simpledb.common;

import simpledb.storage.PageId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

// refer to bustub https://github.com/cmu-db/bustub
// page-level lock
public class LockManager {
    enum LockMode {
        SHARED,
        EXCLUSIVE;
    }

    class LockRequest {
        public int tid;
        public LockMode lockMode;
        public boolean granted = false;

        public LockRequest(int tid, LockMode lockMode, PageId pageId, boolean granted) {
            this.tid = tid;
            this.lockMode = lockMode;
            this.granted = granted;
        }
    }

    class LockRequestQueue {
        public BlockingQueue<LockRequest> requestQueue;
        // coordination
        public Lock latch;
        public Condition condition;
        public int upgradingTid;

        public LockRequestQueue(BlockingQueue<LockRequest> requestQueue, Lock latch, Condition condition, int upgradingTid) {
            this.requestQueue = requestQueue;
            this.latch = latch;
            this.condition = condition;
            this.upgradingTid = upgradingTid;
        }
    }

    private Map<PageId, LockRequestQueue> pageLockMap;
    private Map<Integer, List<Integer>> waitsForMap;
    private Thread cycleDetectionThread;
    private volatile boolean enableCycleDetection;

    public LockManager() {
        enableCycleDetection = true;
        pageLockMap = new ConcurrentHashMap<>();
        waitsForMap = new ConcurrentHashMap<>();
        cycleDetectionThread = new Thread();
    }

    public void lockPage() {

    }

    public void unLockPage() {

    }

}
