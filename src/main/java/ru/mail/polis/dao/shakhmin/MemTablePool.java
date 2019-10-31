package ru.mail.polis.dao.shakhmin;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jetbrains.annotations.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.mail.polis.dao.shakhmin.Cell.EMPTY_DATA;

public class MemTablePool implements Table, Closeable {
    private static final ByteBuffer LOWEST_KEY = ByteBuffer.allocate(0);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile MemTable current;
    private final NavigableMap<Long, Table> pendingToFlush;
    private final NavigableMap<Long, Iterator<Row>> pendingToCompact;
    private final BlockingQueue<TableToFlush> flushQueue;
    private long serialNumber;

    @NotNull private final ExecutorService flusher;
    @NotNull private final Runnable flushingTask;

    private final long flushThresholdInBytes;
    private final AtomicBoolean isClosed;

    /**
     * Construct a new memory table pool for
     * thread safe work with a memory table.
     *
     * @param flushThresholdInBytes threshold of size of Memtable
     * @param startSerialNumber next flushing table serial number
     * @param nThreadsToFlush number of threads to flush tables
     * @param flushingTask task to be performed when a table appears for flushing to disk
     */
    public MemTablePool(final long flushThresholdInBytes,
                        final long startSerialNumber,
                        final int nThreadsToFlush,
                        @NotNull final Runnable flushingTask) {
        this.flushThresholdInBytes = flushThresholdInBytes;
        this.current = new MemTable();
        this.pendingToFlush = new TreeMap<>();
        this.serialNumber = startSerialNumber;
        this.flushQueue = new ArrayBlockingQueue<>(nThreadsToFlush + 1);
        this.isClosed = new AtomicBoolean();
        this.pendingToCompact = new TreeMap<>();

        this.flusher = Executors.newFixedThreadPool(
                nThreadsToFlush,
                new ThreadFactoryBuilder().setNameFormat("flusher-%d").build());
        this.flushingTask = flushingTask;
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Row>> iterators;
        lock.readLock().lock();
        try {
            iterators = Table.joinIterators(current, pendingToFlush, from);
        } finally {
            lock.readLock().unlock();
        }
        return Table.reduceIterators(iterators);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("MemTablePool is already closed");
        }
        setToFlush(key);
        lock.writeLock().lock();
        try {
            current.upsert(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (isClosed.get()) {
            throw new IllegalStateException("MemTablePool is already closed");
        }
        setToFlush(key);
        lock.writeLock().lock();
        try {
            current.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void setToFlush(@NotNull final ByteBuffer key) throws IOException {
        if (current.sizeInBytes()
                + Row.getSizeOfFlushedRow(key, EMPTY_DATA) >= flushThresholdInBytes) {
            TableToFlush tableToFlush = null;
            lock.writeLock().lock();
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
                flusher.execute(flushingTask);
            }
        }
    }

    private void setCompactTableToFlush(@NotNull final Iterator<Row> rows) throws IOException {
        TableToFlush tableToFlush;
        lock.writeLock().lock();
        try {
            tableToFlush = new TableToFlush
                    .Builder(rows, serialNumber)
                    .isCompactTable()
                    .build();
            serialNumber++;
            pendingToCompact.put(serialNumber, rows);
            current = new MemTable();
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(tableToFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flusher.execute(flushingTask);
    }

    @NotNull
    public TableToFlush takeToFlush() throws InterruptedException {
        return flushQueue.take();
    }

    /**
     * Remove a table pending to be flushed to disk.
     *
     * @param serialNumber serial number of table
     */
    public void flushed(final long serialNumber) {
        lock.writeLock().lock();
        try {
            pendingToFlush.remove(serialNumber);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Compact SStables.
     *
     * @param ssTables SStables
     * @throws IOException if an I/O error occurs
     */
    public void compact(@NotNull final NavigableMap<Long, Table> ssTables) throws IOException {
        final List<Iterator<Row>> iterators;
        lock.readLock().lock();
        try {
            iterators = Table.joinIterators(current, ssTables, LOWEST_KEY);
        } finally {
            lock.readLock().unlock();
        }
        setCompactTableToFlush(Table.reduceIterators(iterators));
    }

    /**
     * Remove a compacted table pending to be flushed to disk.
     *
     * @param serialNumber serial number of table
     */
    public void compacted(final long serialNumber) {
        lock.writeLock().lock();
        try {
            pendingToCompact.remove(serialNumber);
        } finally {
            lock.writeLock().unlock();
        }
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
        TableToFlush tableToFlush;
        lock.writeLock().lock();
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
        flusher.execute(flushingTask);
        stopFlushing();
    }

    private void stopFlushing() {
        flusher.shutdown();
        try {
            if (!flusher.awaitTermination(1, TimeUnit.MINUTES)) {
                flusher.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
