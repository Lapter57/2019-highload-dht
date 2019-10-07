package ru.mail.polis.dao.shakhmin;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.mail.polis.dao.shakhmin.Value.EMPTY_DATA;

public class MemTablePool implements Table, Closeable {

    private static final int NUMBER_OF_TABLES_IN_QUEUE = 2;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile MemTable current;
    private NavigableMap<Long, Table> pendingToFlush;
    private BlockingQueue<TableToFlush> flushQueue;
    private long serialNumber;

    private final long flushThresholdInBytes;
    private final AtomicBoolean isClosed;

    public MemTablePool(final long flushThresholdInBytes,
                        final long startSerialNumber) {
        this(flushThresholdInBytes, startSerialNumber, NUMBER_OF_TABLES_IN_QUEUE);
    }

    public MemTablePool(final long flushThresholdInBytes,
                        final long startSerialNumber,
                        final int numberOfTablesInQueue) {
        this.flushThresholdInBytes = flushThresholdInBytes;
        this.current = new MemTable();
        this.pendingToFlush = new TreeMap<>();
        this.serialNumber = startSerialNumber;
        this.flushQueue = new ArrayBlockingQueue<>(numberOfTablesInQueue);
        this.isClosed = new AtomicBoolean();
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) throws IOException {
        lock.readLock().lock();
        final List<Iterator<Row>> iterators;
        try {
            final var memIterator = current.iterator(from);
            iterators = new ArrayList<>();
            iterators.add(memIterator);
            for (final var table: pendingToFlush.descendingMap().values()) {
                iterators.add(table.iterator(from));
            }
        } finally {
            lock.readLock().unlock();
        }
        final var merged = Iterators.mergeSorted(iterators, Row::compareTo);
        final var collapsed = Iters.collapseEquals(merged, Row::getKey);
        return Iterators.filter(collapsed, r -> !r.getValue().isRemoved());
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("MemTablePool is already closed");
        }
        enqueueToFlush(key);
        current.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("MemTablePool is already closed");
        }
        enqueueToFlush(key);
        current.remove(key);
    }

    private void enqueueToFlush(@NotNull final ByteBuffer key) {
        if (current.sizeInBytes()
                + Row.getSizeOfFlushedRow(key, EMPTY_DATA) >= flushThresholdInBytes) {
            lock.writeLock().lock();
            TableToFlush tableToFlush = null;
            try {
                if (current.sizeInBytes()
                        + Row.getSizeOfFlushedRow(key, EMPTY_DATA) >= flushThresholdInBytes) {
                    tableToFlush = new TableToFlush(serialNumber, current);
                    pendingToFlush.put(serialNumber, current);
                    serialNumber++;
                    current = new MemTable();
                }
            } finally {
                lock.writeLock().unlock();
            }
            if (tableToFlush != null) {
                try {
                    flushQueue.put(tableToFlush);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @NotNull
    public TableToFlush takeToFlush() throws InterruptedException {
        return flushQueue.take();
    }

    public void flushed(final long serialNumber) {
        lock.writeLock().lock();
        try {
            pendingToFlush.remove(serialNumber);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean hasTablesToFlush() {
        return !flushQueue.isEmpty();
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            long sizeInBytes = current.sizeInBytes();
            for (final var table : pendingToFlush.values()) {
                sizeInBytes += table.sizeInBytes();
            }
            return sizeInBytes;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long serialNumber() {
        lock.readLock().lock();
        try {
            return serialNumber;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        lock.writeLock().lock();
        TableToFlush tableToFlush;
        try {
            tableToFlush = new TableToFlush(serialNumber, current, true);
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(tableToFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
