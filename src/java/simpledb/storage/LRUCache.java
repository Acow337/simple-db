package simpledb.storage;

import simpledb.common.DbException;

import java.util.*;

public class LRUCache<K, T> {
    class DLinkedNode {
        PageId key;
        Page value;
        DLinkedNode prev;
        DLinkedNode next;

        public DLinkedNode() {
        }

        public DLinkedNode(PageId _key, Page _value) {
            key = _key;
            value = _value;
        }
    }

    private Map<PageId, DLinkedNode> cache = new HashMap<>();
    private int size;
    private int capacity;
    private DLinkedNode head, tail;

    public LRUCache(int capacity) {
//        System.out.println("LRUCache capacity: " + capacity);
        this.size = 0;
        this.capacity = capacity;
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        tail.prev = head;
    }

    public boolean containsKey(PageId key) {
        return cache.containsKey(key);
    }

    public synchronized void remove(PageId key) {
//        System.out.println("LRUCache: remove: " + key.getPageNumber());

        if (!cache.containsKey(key)) {
            return;
        }
//        System.out.println("LRUCatch: remove " + key);
        DLinkedNode node = cache.get(key);
        removeNode(node);
        cache.remove(key);
        size--;
    }

    public Iterator<Page> valueIterator() {
        List<Page> list = new ArrayList<>(size);
        for (DLinkedNode node : cache.values()) {
            list.add(node.value);
        }
        return list.iterator();
    }

    public void clear() {
        cache.clear();
        size = 0;
    }

    public Page get(PageId key) {
//        System.out.println("Get: " + key + " " + toString());
        DLinkedNode node = cache.get(key);
        if (node == null) {
            return null;
        }
        moveToHead(node);
        return node.value;
    }

    public Set<PageId> getIds() {
        return cache.keySet();
    }

    public synchronized Page put(PageId key, Page value) throws DbException {
//        System.out.println("PUT: " + key);
        DLinkedNode node = cache.get(key);
//        System.out.println("put: " + key);
        DLinkedNode remove = null;
        if (node == null) {
            DLinkedNode newNode = new DLinkedNode(key, value);
            if (size >= capacity) {
                DLinkedNode tail = removeTail();
                remove = cache.remove(tail.key);
//                System.out.printf("oversize: remove a node, capacity %d %s \n ", capacity, remove.value.getId());
                --size;
            }
//            System.out.println("put key: " + key.getPageNumber());
            addToHead(newNode);
            cache.put(key, newNode);
            ++size;
//            System.out.println("after remove and add: " + this);
            if (remove != null) return remove.value;
        } else {
            node.value = value;
            moveToHead(node);
        }
        return null;
    }

    private void addToHead(DLinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(DLinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private synchronized void moveToHead(DLinkedNode node) {
        removeNode(node);
        addToHead(node);
    }

    private DLinkedNode removeTail() throws DbException {
//        System.out.println("removeTail :" + toString());
        DLinkedNode res = tail.prev;
        while (res.value != null && res.value.isDirty() != null) {
//            System.out.println(res.value.getId() + " is dirty, change to another one");
            res = res.prev;
            if (res.value == null) throw new DbException("LRU error");
        }
//        System.out.println(res.value.getId().getPageNumber() + " gonna be removed");
        removeNode(res);
        return res;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        DLinkedNode node = head;
        while (node != null) {
            if (node.prev == null) {
                sb.append("head->");
            } else if (node.next == null) {
                sb.append("tail");
            } else {
                sb.append(node.value.getId().getPageNumber());
                if (node.value.isDirty() != null) {
                    sb.append("(D)");
                }
                sb.append("->");
            }
            node = node.next;
        }
        sb.append(String.format(" (size: %d, capacity: %d)", size, capacity));
        return sb.toString();
    }

}
