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
        public PageNode next;
        public PageNode prev;
        public K key;
        public V value;

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
}
