package ru.mail.polis.dao.shakhmin;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class LSMDao implements DAO {
    private static final Logger log = LoggerFactory.getLogger(LSMDao.class);

    private static final String SUFFIX = ".bin";
    private static final String PREFIX = "SSTable_";
    private static final String REGEX = PREFIX + "\\d+" + SUFFIX;

    @NotNull private MemTablePool memTable;
    @NotNull private NavigableMap<Long, Table> ssTables = new ConcurrentSkipListMap<>();
    @NotNull private final File flushDir;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    class FlushingTask implements Runnable {

        @Override
        public void run() {
                TableToFlush tableToFlush = null;
                try {
                    tableToFlush = memTable.takeToFlush();
                    final long serialNumber = tableToFlush.getSerialNumber();
                    final boolean poisonReceived = tableToFlush.isPoisonPill();
                    final boolean isCompactTable = tableToFlush.isCompactTable();
                    final var table = tableToFlush.getTable();
                    if (poisonReceived || isCompactTable) {
                        flush(serialNumber, table);
                    } else {
                        flushAndLoad(serialNumber, table);
                    }
                    if (isCompactTable) {
                        completeCompaction(serialNumber);
                        memTable.compacted(serialNumber);
                    } else {
                        memTable.flushed(serialNumber);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    log.error("Error while flush generation " + tableToFlush.getSerialNumber(), e);
                }
        }
    }

    public LSMDao(@NotNull final File flushDir,
                  final long flushThresholdInBytes) throws IOException {
        this(flushDir, flushThresholdInBytes, Runtime.getRuntime().availableProcessors() + 1);
    }

    /**
     * Constructs a new DAO based on LSM tree.
     *
     * @param flushDir local disk folder to persist the data to
     * @param flushThresholdInBytes threshold of size of Memtable
     * @param nThreadsToFlush number of threads to flush tables
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    public LSMDao(@NotNull final File flushDir,
                  final long flushThresholdInBytes,
                  final int nThreadsToFlush) throws IOException {
        this.flushDir = flushDir;
        final var serialNumberSStable = new AtomicLong();
        Files.walkFileTree(flushDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path path,
                    final BasicFileAttributes attrs) throws IOException {
                final var file = path.toFile();
                if (file.getName().matches(REGEX)) {
                    final var fileName = Iterables.get(Splitter.on(".").split(file.getName()), 0);
                    final long serialNumber = Long.parseLong(Iterables.get(Splitter.on('_').split(fileName), 1));
                    serialNumberSStable.set(
                            Math.max(serialNumberSStable.get(), serialNumber + 1L));
                    ssTables.put(serialNumber, new SSTable(file.toPath(), serialNumber));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        log.info("Number of threads to flush = {}", nThreadsToFlush);
        this.memTable = new MemTablePool(
                flushThresholdInBytes,
                serialNumberSStable.get(),
                nThreadsToFlush,
                new FlushingTask());
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final var collapsed = rowsIterator(from);
        final var alive = Iterators.filter(collapsed, r -> !r.getCell().isRemoved());
        return Iterators.transform(alive,
                r -> Record.of(r.getKey(), r.getCell().getData()));
    }

    @NotNull
    private Iterator<Row> rowsIterator(@NotNull final ByteBuffer from) throws IOException {
        final var iterators = Table.joinIterators(memTable, ssTables, from);
        return Table.reduceIterators(iterators);
    }

    /**
     *  Get cell by key.
     *
     * @param key key
     * @return null if cell is not found and cell otherwise
     * @throws IOException if an I/O error occurs
     */
    @Nullable
    public Cell getCell(@NotNull final ByteBuffer key) throws IOException {
        final var iter = rowsIterator(key);
        if (!iter.hasNext()) {
            return null;
        }
        final var row = iter.next();
        if (!row.getKey().equals(key)) {
            return null;
        }
        return row.getCell();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        memTable.close();
    }

    private void flush(final long serialNumber,
                       @NotNull final Iterator<Row> rowsIterator) throws IOException {
        log.info("Flushing generation [{}]...", serialNumber);
        SSTable.flush(
                Path.of(flushDir.getAbsolutePath(), PREFIX + serialNumber + SUFFIX),
                rowsIterator);
        log.info("Flushing generation [{}] done", serialNumber);
    }

    private void flushAndLoad(final long serialNumber,
                              @NotNull final Iterator<Row> rowsIterator) throws IOException {
        log.info("Flushing generation [{}]...", serialNumber);
        final var path = Path.of(flushDir.getAbsolutePath(), PREFIX + serialNumber + SUFFIX);
        SSTable.flush(path, rowsIterator);
        ssTables.put(serialNumber,
                new SSTable(
                        path.toAbsolutePath(),
                        serialNumber));
        log.info("Flushing generation [{}] done", serialNumber);
    }

    @Override
    public void compact() throws IOException {
        log.info("Need compaction");
        memTable.compact(ssTables);
    }

    private void completeCompaction(final long serialNumber) throws IOException {
        log.info("Compaction is done. Serial number of compact table is [{}]", serialNumber);
        lock.writeLock().lock();
        try {
            ssTables = new ConcurrentSkipListMap<>();
        } finally {
            lock.writeLock().unlock();
        }
        cleanDirectory(serialNumber);
    }

    private void cleanDirectory(final long serialNumber) throws IOException {
        lock.writeLock().lock();
        try {
            Files.walkFileTree(flushDir.toPath(), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(
                        final Path path,
                        final BasicFileAttributes attrs) throws IOException {
                    final File file = path.toFile();
                    if (file.getName().matches(REGEX)) {
                        final var fileName = Iterables.get(Splitter.on(".").split(file.getName()), 0);
                        final long sn = Long.parseLong(Iterables.get(Splitter.on('_').split(fileName), 1));
                        if (sn >= serialNumber) {
                            ssTables.put(sn, new SSTable(file.toPath(), sn));
                            return FileVisitResult.CONTINUE;
                        }
                    }
                    Files.delete(path);
                    return FileVisitResult.CONTINUE;
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }
}
