package simpledb.utils;


import java.util.HashMap;
import java.util.Map;

/*
 * LRUCache is A BufferPool Area containing several pages, where the pages are
 * replaced using LRU algorithm. It's a relatively complex data structure.
 * An "LRUCache" is made up of many "PageNode".
 * */
public class LRUCache<K, V> {
    public class PageNode {
        private PageNode next;
        private PageNode prev;
        private K key;
        private V value;

        public PageNode(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private int MaxSize;
    private Map<K, PageNode> LRUMap;
    private PageNode head;
    private PageNode tail;

    public LRUCache(int maxSize) {
        this.MaxSize = maxSize;
        this.head = new PageNode(null, null);
        this.tail = new PageNode(null, null);
        this.head.next = tail;
        this.tail.prev = head;
        this.LRUMap = new HashMap<>();
    }

    /*
     *  LRUCache is implemented in the form of double linked list, with limit in size.
     *  In LRU Algorithm, we scan the list in order. When a page is queried, it will be moved to
     *  the first place in the list. Also, a page will be put in the first place if it is inserted
     *  into the list. Therefore, the methods "moveToHead" and "addToHead" and "removeNode" are implemented
     * */
    public void addToHead(PageNode node) {
        node.next = this.head.next;
        this.head.next.prev = node;
        node.prev = this.head;
        this.head.next = node;
    }

    public void removeNode(PageNode node) {
        if (node.prev != null && node.next != null) {
            //gc will automatically release the occupied memory!!!
            node.next.prev = node.prev;
            node.prev.next = node.next;
        }
    }

    public void moveToHead(PageNode node) {
        removeNode(node);
        addToHead(node);
    }

    /*
     * The methods "put" and "get" display the central part of the LRU algorithm.
     * */
    public synchronized V get(K key) {
        //V is actually "Page".
        if (LRUMap.containsKey(key)) {
            PageNode node = LRUMap.get(key);
            moveToHead(node);
            return node.value;
        }
        return null;
    }

    public synchronized void put(K key, V value) {
        if (LRUMap.containsKey(key)) {
            PageNode node = LRUMap.get(key);
            node.value = value;
            moveToHead(node);
        } else {
            PageNode newNode = new PageNode(key, value);
            LRUMap.put(key, newNode);
            addToHead(newNode);
        }
    }

    public synchronized void remove(K key) {
        if (LRUMap.containsKey(key)) {
            PageNode node = LRUMap.get(key);
            LRUMap.remove(key);
            removeNode(node);
        }
    }

    public int getMaxSize() {
        return MaxSize;
    }

    public int getSize() {
        return LRUMap.size();
    }
}




