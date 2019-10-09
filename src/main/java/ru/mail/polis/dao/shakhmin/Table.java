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
 *<p>
 * Each instance of this interface must have a serial number,
 * which indicates the relevance of the storing data.
 * </p>
 */
public interface Table {
    ByteBuffer LOWEST_KEY = ByteBuffer.allocate(0);

    @NotNull
    Iterator<Row> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) throws IOException;

    void remove(@NotNull ByteBuffer key) throws IOException;

    long sizeInBytes();

    long serialNumber();

    static List<Iterator<Row>> combineTables(@NotNull final Table table,
                                             @NotNull final NavigableMap<Long, Table> otherTables,
                                             @NotNull final ByteBuffer from) throws IOException {
        final var memIterator = table.iterator(from);
        final List<Iterator<Row>> iterators = new ArrayList<>();
        iterators.add(memIterator);
        for (final var entity: otherTables.descendingMap().values()) {
            iterators.add(entity.iterator(from));
        }
        return iterators;
    }

    static Iterator<Row> transformRows(@NotNull final List<Iterator<Row>> iterators) {
        final var merged = Iterators.mergeSorted(iterators, Row::compareTo);
        final var collapsed = Iters.collapseEquals(merged, Row::getKey);
        return Iterators.filter(collapsed, r -> !r.getValue().isRemoved());
    }
}
