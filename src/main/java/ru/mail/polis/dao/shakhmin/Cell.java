package ru.mail.polis.dao.shakhmin;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

public final class Cell implements Comparable<Cell> {

    public static final ByteBuffer EMPTY_DATA = ByteBuffer.allocate(0);

    private final long timestamp;

    @NotNull
    private final ByteBuffer data;

    private Cell(final long timestamp,
                 @NotNull final ByteBuffer data) {
        this.timestamp = timestamp;
        this.data = data;
    }

    @NotNull
    public static Cell of(final long timestamp,
                          @NotNull final ByteBuffer data) {
        return new Cell(timestamp, data);
    }

    @NotNull
    public static Cell tombstone(final long timestamp) {
        return new Cell(-timestamp, EMPTY_DATA);
    }

    @NotNull
    public ByteBuffer getData() {
        return data.asReadOnlyBuffer();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isRemoved() {
        return timestamp < 0;
    }

    @Override
    public int compareTo(@NotNull final Cell cell) {
        return Long.compare(Math.abs(cell.timestamp), Math.abs(timestamp));
    }
}
