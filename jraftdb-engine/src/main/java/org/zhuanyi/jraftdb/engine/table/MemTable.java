package org.zhuanyi.jraftdb.engine.table;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.zhuanyi.common.Slice;
import org.zhuanyi.jraftdb.engine.comparator.InternalKeyComparator;
import org.zhuanyi.jraftdb.engine.data.*;
import org.zhuanyi.jraftdb.engine.table.iterator.InternalIterator;
import org.zhuanyi.jraftdb.engine.table.iterator.SeekingIterable;
import org.zhuanyi.jraftdb.engine.table.iterator.SeekingIterator;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.zhuanyi.common.SizeOf.SIZE_OF_LONG;

public class MemTable implements SeekingIterable<InternalKey, Slice> {

    // ConcurrentSkipListMap是如何实现无锁并发的
    private final ConcurrentSkipListMap<InternalKey, Slice> table;

    private final AtomicLong approximateMemoryUsage = new AtomicLong();

    public MemTable(InternalKeyComparator internalKeyComparator) {
        table = new ConcurrentSkipListMap<>(internalKeyComparator);
    }

    @Override
    public SeekingIterator<InternalKey, Slice> iterator() {
        return null;
    }

    public boolean isEmpty() {
        return table.isEmpty();
    }

    public long approximateMemoryUsage() {
        return approximateMemoryUsage.get();
    }

    /**
     * 插入一个数据
     *
     * @param sequenceNumber
     * @param valueType
     * @param key
     * @param value
     */
    public void add(long sequenceNumber, ValueType valueType, Slice key, Slice value) {
        requireNonNull(valueType, "valueType is null");
        requireNonNull(key, "key is null");
        requireNonNull(valueType, "valueType is null");

        InternalKey internalKey = new InternalKey(key, sequenceNumber, valueType);
        table.put(internalKey, value);

        approximateMemoryUsage.addAndGet(key.length() + SIZE_OF_LONG + value.length());
    }

    /**
     * 查询一个数据
     *
     * @param key
     * @return
     */
    public LookupResult get(LookupKey key) {
        requireNonNull(key, "key is null");

        InternalKey internalKey = key.getInternalKey();
        Map.Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
        if (entry == null) {
            return null;
        }

        InternalKey entryKey = entry.getKey();
        if (entryKey.getUserKey().equals(key.getUserKey())) {
            if (entryKey.getValueType() == ValueType.DELETION) {
                return LookupResult.deleted(key);
            } else {
                return LookupResult.ok(key, entry.getValue());
            }
        }
        return null;
    }

    public class MemTableIterator implements InternalIterator {

        private PeekingIterator<Map.Entry<InternalKey, Slice>> iterator;

        public MemTableIterator() {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public void seekToFirst() {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public void seek(InternalKey targetKey) {
            iterator = Iterators.peekingIterator(table.tailMap(targetKey).entrySet().iterator());
        }

        @Override
        public Map.Entry<InternalKey, Slice> peek() {
            Map.Entry<InternalKey, Slice> entry = iterator.peek();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public Map.Entry<InternalKey, Slice> next() {
            Map.Entry<InternalKey, Slice> entry = iterator.next();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
