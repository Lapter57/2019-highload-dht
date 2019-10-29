package ru.mail.polis.dao.shakhmin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;

/**
 * A sorted collection for storing rows ({@link Row}).
 * Each instance of this interface must have a serial number,
 * which indicates the relevance of the storing data.
 */
public interface Table {

    @NotNull
    Iterator<Row> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) throws IOException;

    void remove(@NotNull ByteBuffer key) throws IOException;

    long sizeInBytes();

    long serialNumber();

    /**
     * Join table iterators.
     *
     * @param memTable memory table
     * @param tables other tables with a serial number
     * @param from starting position for iterators
     * @return list of iterators of all tables
     * @throws IOException if an I/O error occurs
     */
    static List<Iterator<Row>> joinIterators(@NotNull final Table memTable,
                                             @NotNull final NavigableMap<Long, Table> tables,
                                             @NotNull final ByteBuffer from) throws IOException {
        final var memIterator = memTable.iterator(from);
        final var iterators = new ArrayList<Iterator<Row>>();
        iterators.add(memIterator);
        for (final var entity: tables.descendingMap().values()) {
            iterators.add(entity.iterator(from));
        }
        return iterators;
    }

    /**
     * Returns an iterator over the merged collapsed
     * contents of all given {@code iterators}.
     *
     * @param iterators list of iterators
     * @return iterator of unique rows
     */
    static Iterator<Row> reduceIterators(@NotNull final List<Iterator<Row>> iterators) {
        final var merged = Iterators.mergeSorted(iterators, Row::compareTo);
        return Iters.collapseEquals(merged, Row::getKey);
    }
}
