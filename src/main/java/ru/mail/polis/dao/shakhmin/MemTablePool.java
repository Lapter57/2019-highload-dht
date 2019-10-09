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
    private final AtomicBoolean isPendingCompaction;
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
        this.isPendingCompaction = new AtomicBoolean();
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) throws IOException {
        lock.readLock().lock();
        final List<Iterator<Row>> iterators;
        try {
            iterators = Table.combineTables(current, pendingToFlush, from);
        } finally {
            lock.readLock().unlock();
        }
        return Table.transformRows(iterators);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("MemTablePool is already closed");
        }
        setToFlush(key);
        current.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("MemTablePool is already closed");
        }
        setToFlush(key);
        current.remove(key);
    }

    private void setToFlush(@NotNull final ByteBuffer key) throws IOException {
        if (current.sizeInBytes()
                + Row.getSizeOfFlushedRow(key, EMPTY_DATA) >= flushThresholdInBytes) {
            lock.writeLock().lock();
            TableToFlush tableToFlush = null;
            try {
                if (current.sizeInBytes()
                        + Row.getSizeOfFlushedRow(key, EMPTY_DATA) >= flushThresholdInBytes) {
                    tableToFlush = TableToFlush.of(current.iterator(LOWEST_KEY), serialNumber);
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

    private void setCompactTableToFlush(@NotNull final Iterator<Row> rows) throws IOException {
        lock.writeLock().lock();
        TableToFlush tableToFlush;
        try {
            tableToFlush = new TableToFlush
                    .Builder(rows, serialNumber)
                    .isCompactTable()
                    .build();
            serialNumber++;
            isPendingCompaction.compareAndSet(false, true);
            current = new MemTable();
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(tableToFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

    public void compact(@NotNull final NavigableMap<Long, Table> ssTables) throws IOException {
        lock.readLock().lock();
        final List<Iterator<Row>> iterators;
        try {
            iterators = Table.combineTables(current, ssTables, LOWEST_KEY);
        } finally {
            lock.readLock().unlock();
        }
        setCompactTableToFlush(Table.transformRows(iterators));
    }

    public void compacted() {
        isPendingCompaction.compareAndSet(true, false);
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
            tableToFlush = new TableToFlush
                    .Builder(current.iterator(LOWEST_KEY), serialNumber)
                    .poisonPill()
                    .build();
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
