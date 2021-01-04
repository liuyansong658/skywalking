package ch.qos.logback.classic.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.spi.MDCAdapter;

public final class LogbackMDCAdapter
        implements MDCAdapter
{
    final InheritableThreadLocal<Map<String, String>> copyOnInheritThreadLocal;
    private static final int WRITE_OPERATION = 1;
    private static final int READ_OPERATION = 2;
    final ThreadLocal<Integer> lastOperation;

    public LogbackMDCAdapter()
    {
        this.copyOnInheritThreadLocal = new InheritableThreadLocal();

        this.lastOperation = new ThreadLocal(); }

    private Integer getAndSetLastOperation(int op) {
        Integer lastOp = (Integer)this.lastOperation.get();
        this.lastOperation.set(Integer.valueOf(op));
        return lastOp;
    }

    private boolean wasLastOpReadOrNull(Integer lastOp) {
        return (lastOp == null) || (lastOp.intValue() == 2);
    }

    private Map<String, String> duplicateAndInsertNewMap(Map<String, String> oldMap) {
        Map newMap = Collections.synchronizedMap(new HashMap());
        if (oldMap != null)
        {
            synchronized (oldMap) {
                newMap.putAll(oldMap);
            }
        }

        this.copyOnInheritThreadLocal.set(newMap);
        return newMap;
    }

    public void put(String key, String val)
            throws IllegalArgumentException
    {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }

        Map oldMap = (Map)this.copyOnInheritThreadLocal.get();
        Integer lastOp = getAndSetLastOperation(1);

        if ((wasLastOpReadOrNull(lastOp)) || (oldMap == null)) {
            Map newMap = duplicateAndInsertNewMap(oldMap);
            newMap.put(key, val);
        } else {
            oldMap.put(key, val);
        }
    }

    public void remove(String key)
    {
        if (key == null) {
            return;
        }
        Map oldMap = (Map)this.copyOnInheritThreadLocal.get();
        if (oldMap == null) return;

        Integer lastOp = getAndSetLastOperation(1);

        if (wasLastOpReadOrNull(lastOp)) {
            Map newMap = duplicateAndInsertNewMap(oldMap);
            newMap.remove(key);
        } else {
            oldMap.remove(key);
        }
    }

    public void clear()
    {
        this.lastOperation.set(Integer.valueOf(1));
        this.copyOnInheritThreadLocal.remove();
    }

    public String get(String key)
    {
        Map map = getPropertyMap();
        if ((map != null) && (key != null)) {
            return (String)map.get(key);
        }
        return null;
    }

    public Map<String, String> getPropertyMap()
    {
        this.lastOperation.set(Integer.valueOf(2));
        return (Map)this.copyOnInheritThreadLocal.get();
    }

    public Set<String> getKeys()
    {
        Map map = getPropertyMap();

        if (map != null) {
            return map.keySet();
        }
        return null;
    }

    public Map getCopyOfContextMap()
    {
        this.lastOperation.set(Integer.valueOf(2));
        Map hashMap = (Map)this.copyOnInheritThreadLocal.get();
        if (hashMap == null) {
            return null;
        }
        return new HashMap(hashMap);
    }

    public void setContextMap(Map contextMap)
    {
        this.lastOperation.set(Integer.valueOf(1));

        Map newMap = Collections.synchronizedMap(new HashMap());
        newMap.putAll(contextMap);

        this.copyOnInheritThreadLocal.set(newMap);
    }
}