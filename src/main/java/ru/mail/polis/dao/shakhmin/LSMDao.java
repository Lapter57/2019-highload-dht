package ru.mail.polis.dao.shakhmin;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


public final class LSMDao implements DAO {
    private static final Logger log = LoggerFactory.getLogger(LSMDao.class);

    private static final String SUFFIX = ".bin";
    private static final String PREFIX = "SSTable_";
    private static final String REGEX = PREFIX + "\\d+" + SUFFIX;

    @NotNull private MemTablePool memTable;
    @NotNull private NavigableMap<Long, Table> ssTables = new ConcurrentSkipListMap<>();
    @NotNull private final File flushDir;
    @NotNull private final ExecutorService flusher;
    @NotNull private final Runnable flushingTask;

    class FlushingTask implements Runnable {

        @Override
        public void run() {
            boolean poisonReceived = false;
            while (!poisonReceived && !Thread.currentThread().isInterrupted()) {
                TableToFlush tableToFlush = null;
                try {
                    tableToFlush = memTable.takeToFlush();
                    final long serialNumber = tableToFlush.getSerialNumber();
                    poisonReceived = tableToFlush.isPoisonPill();
                    final boolean isCompactTable = tableToFlush.isCompactTable();
                    final var table = tableToFlush.getTable();
                    if (poisonReceived || isCompactTable) {
                        flush(serialNumber, table);
                    } else {
                        flushAndLoad(serialNumber, table);
                    }
                    if (isCompactTable) {
                        completeCompaction(serialNumber);
                        memTable.compacted();
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
    }

    public LSMDao(
            @NotNull final File flushDir,
            final long flushThresholdInBytes) throws IOException {
        this(flushDir, flushThresholdInBytes, Runtime.getRuntime().availableProcessors() - 2);
    }

    /**
     * Constructs a new DAO based on LSM tree.
     *
     * @param flushDir local disk folder to persist the data to
     * @param flushThresholdInBytes threshold of size of Memtable
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    public LSMDao(
            @NotNull final File flushDir,
            final long flushThresholdInBytes,
            final int nThreadsToFlush) throws IOException {
        this.flushDir = flushDir;
        final var serialNumberSStable = new AtomicLong();
        Files.walkFileTree(flushDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path path,
                    final BasicFileAttributes attrs) throws IOException {
                final File file = path.toFile();
                if (file.getName().matches(REGEX)) {
                    final String fileName = file.getName().split("\\.")[0];
                    final long serialNumber = Long.parseLong(fileName.split("_")[1]);
                    serialNumberSStable.set(
                            Math.max(serialNumberSStable.get(), serialNumber + 1L));
                    ssTables.put(serialNumber, new SSTable(file.toPath(), serialNumber));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        this.memTable = new MemTablePool(flushThresholdInBytes, serialNumberSStable.get());
        this.flushingTask = new FlushingTask();
        this.flusher = Executors.newSingleThreadExecutor();
        flusher.execute(flushingTask);
        //this.flusher = Executors.newFixedThreadPool(nThreadsToFlush);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final var alive = rowsIterator(from);
        return Iterators.transform(alive,
                r -> Record.of(r.getKey(), r.getValue().getData()));
    }

    @NotNull
    private Iterator<Row> rowsIterator(@NotNull final ByteBuffer from) throws IOException {
        final var iterators = Table.combineTables(memTable, ssTables, from);
        return Table.transformRows(iterators);
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
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
        flusher.shutdown();
        try {
            if (!flusher.awaitTermination(1, TimeUnit.MINUTES)) {
                flusher.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
        ssTables = new ConcurrentSkipListMap<>();
        cleanDirectory(serialNumber);
    }

    private void cleanDirectory(final long serialNumber) throws IOException {
        Files.walkFileTree(flushDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path path,
                    final BasicFileAttributes attrs) throws IOException {
                final File file = path.toFile();
                if (file.getName().matches(REGEX)) {
                    final String fileName = file.getName().split("\\.")[0];
                    final long sn = Long.parseLong(fileName.split("_")[1]);
                    if (sn >= serialNumber) {
                        ssTables.put(sn, new SSTable(file.toPath(), sn));
                        return FileVisitResult.CONTINUE;
                    }
                }
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
