package ru.mail.polis.dao.shakhmin;

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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;


import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;

import static ru.mail.polis.dao.shakhmin.Value.EMPTY_DATA;


public final class LSMDao implements DAO {

    private static final String SUFFIX = ".bin";
    private static final String PREFIX = "SSTable_";
    private static final String REGEX = PREFIX + "\\d+" + SUFFIX;
    private static final ByteBuffer LOWEST_KEY = ByteBuffer.allocate(0);

    @NotNull private final MemTable memTable = new MemTable();
    @NotNull private List<Table> ssTables = new ArrayList<>();
    @NotNull private final File flushDir;
    @NotNull private final AtomicLong serialNumberSStable;
    private final long flushThresholdInBytes;

    /**
     * Constructs a new DAO based on LSM tree.
     *
     * @param flushDir local disk folder to persist the data to
     * @param flushThresholdInBytes threshold of size of Memtable
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    public LSMDao(
            @NotNull final File flushDir,
            final long flushThresholdInBytes) throws IOException {
        this.flushThresholdInBytes = flushThresholdInBytes;
        this.flushDir = flushDir;
        this.serialNumberSStable = new AtomicLong();
        Files.walkFileTree(flushDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path path,
                    final BasicFileAttributes attrs) throws IOException {
                final File file = path.toFile();
                if (file.getName().matches(REGEX)) {
                    final String fileName = file.getName().split("\\.")[0];
                    final long serialNumber = Long.valueOf(fileName.split("_")[1]);
                    serialNumberSStable.set(
                            Math.max(serialNumberSStable.get(), serialNumber + 1L));
                    ssTables.add(new SSTable(file.toPath(), serialNumber));
                }
                return FileVisitResult.CONTINUE;
            }
        });
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
        final var memIterator = memTable.iterator(from);
        final var iterators = new ArrayList<Iterator<Row>>();
        iterators.add(memIterator);
        for (final var ssTable: ssTables) {
            iterators.add(ssTable.iterator(from));
        }
        final var merged = Iterators.mergeSorted(iterators, Row::compareTo);
        final var collapsed = Iters.collapseEquals(merged, Row::getKey);
        return Iterators.filter(collapsed, r -> !r.getValue().isRemoved());
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws IOException {
        if (memTable.sizeInBytes()
                + Row.getSizeOfFlushedRow(key, value) >= flushThresholdInBytes) {
            flushAndLoad(memTable.iterator(LOWEST_KEY));
        }
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (memTable.sizeInBytes()
                + Row.getSizeOfFlushedRow(key, EMPTY_DATA) >= flushThresholdInBytes) {
            flushAndLoad(memTable.iterator(LOWEST_KEY));
        }
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        if (memTable.sizeInBytes() != 0L) {
            flush(memTable.iterator(LOWEST_KEY));
        }
    }

    private void flush(@NotNull final Iterator<Row> rowsIterator) throws IOException {
        final var fileName = nameFlushedTable();
        SSTable.flush(
                Path.of(flushDir.getAbsolutePath(), fileName + SUFFIX),
                rowsIterator);
        memTable.clear();
    }

    private void flushAndLoad(@NotNull final Iterator<Row> rowsIterator) throws IOException {
        final var path = Path.of(flushDir.getAbsolutePath(),
                nameFlushedTable() + SUFFIX);
        SSTable.flush(path, rowsIterator);
        ssTables.add(new SSTable(
                path.toAbsolutePath(),
                serialNumberSStable.get() - 1L));
        memTable.clear();
    }

    @NotNull
    private String nameFlushedTable() {
        return PREFIX + serialNumberSStable.getAndIncrement();
    }

    @Override
    public void compact() throws IOException {
        final var iterator = rowsIterator(LOWEST_KEY);
        flush(iterator);
        clearAll();
    }

    private void clearAll() throws IOException {
        memTable.clear();
        ssTables = new ArrayList<>();
        cleanDirectory();
    }

    private void cleanDirectory() throws IOException {
        Files.walkFileTree(flushDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(
                    final Path path,
                    final BasicFileAttributes attrs) throws IOException {
                final File file = path.toFile();
                if (file.getName().matches(REGEX)) {
                    final String fileName = file.getName().split("\\.")[0];
                    final long serialNumber = Long.valueOf(fileName.split("_")[1]);
                    if (serialNumber == serialNumberSStable.get() - 1L) {
                        ssTables.add(new SSTable(file.toPath(), serialNumber));
                        return FileVisitResult.CONTINUE;
                    }
                }
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
