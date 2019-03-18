package com.lin.commons.utils;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author jianglinzou
 * @date 2019/3/9 下午9:24
 */
public class ConcurrentHashSet<E> extends AbstractSet<E> {

    private static final long serialVersionUID = -8347878570391674042L;

    protected final Map<E, Boolean> map;

    public ConcurrentHashSet() {
        this.map = new ConcurrentHashMap<E, Boolean>();
    }


    public ConcurrentHashSet(ConcurrentHashMap<E, Boolean> map, Collection<E> c) {
        this.map = map;
        this.addAll(c);
    }




    public int size() {
        return this.map.size();
    }

    public boolean contains(Object o) {
        return this.map.containsKey(o);
    }

    public Iterator<E> iterator() {
        return this.map.keySet().iterator();
    }

    public boolean remove(Object o) {
        return this.map.remove(o) != null;
    }

    public void clear() {
        this.map.clear();
    }


    public boolean add(E o) {
        Boolean answer = (Boolean)((ConcurrentMap)this.map).putIfAbsent(o, Boolean.TRUE);
        return answer == null;
    }


}
