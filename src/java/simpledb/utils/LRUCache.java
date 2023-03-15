package simpledb.utils;


import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * LRUCache is A BufferPool Area containing several pages, where the pages are
 * replaced using LRU algorithm. It's a relatively complex data structure.
 * An "LRUCache" is made up of many "PageNode".
 */
public class LRUCache {
    public class PageNode {
        private PageNode next;
        private PageNode prev;
        private Page value;

        public PageNode(Page value) {
            this.value = value;
        }
    }

    private int MaxSize;
    private Map<PageId, PageNode> LRUMap;
    private PageNode head;
    private PageNode tail;

    public LRUCache(int maxSize) {
        this.MaxSize = maxSize;
        this.head = new PageNode(null);
        this.tail = new PageNode(null);
        this.head.next = tail;
        this.tail.prev = head;
        this.LRUMap = new ConcurrentHashMap<>();
    }

    /**
     * LRUCache is implemented in the form of double linked list, with limit in size.
     * In LRU Algorithm, we scan the list in order. When a page is queried, it will be moved to
     * the first place in the list. Also, a page will be put in the first place if it is inserted
     * into the list. Therefore, the methods "moveToHead" and "addToHead" and "removeNode" are implemented
     */
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

    /**
     * The methods "put" and "get" display the central part of the LRU algorithm.
     */
    public synchronized Page get(PageId key) {
        //V is actually "Page".
        if (LRUMap.containsKey(key)) {
            PageNode node = LRUMap.get(key);
            moveToHead(node);
            return node.value;
        }
        return null;
    }

    public synchronized void put(PageId key, Page value) {
        if (LRUMap.containsKey(key)) {
            PageNode node = LRUMap.get(key);
            node.value = value;
            moveToHead(node);
        } else {
            PageNode newNode = new PageNode(value);
            LRUMap.put(key, newNode);
            addToHead(newNode);
        }
    }

    public synchronized void remove(PageId key) {
        if (LRUMap.containsKey(key)) {
            PageNode node = LRUMap.get(key);
            LRUMap.remove(key);
            removeNode(node);
        }
    }

    public synchronized Iterator<Page> valueIterator() {
        Collection<PageNode> values = LRUMap.values();
        List<Page> list = values.stream().map(x -> x.value).collect(Collectors.toList());
        return list.iterator();
    }

    public synchronized Iterator<Page> reverseIterator() {
        PageNode node = tail.prev;
        List<Page> list = new ArrayList<>();
        while (!node.equals(head)) {
            list.add(node.value);
            node = node.prev;
        }
        return list.iterator();
    }

    public int getMaxSize() {
        return MaxSize;
    }

    public int getSize() {
        return LRUMap.size();
    }
}




