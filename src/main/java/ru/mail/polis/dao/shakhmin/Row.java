package ru.mail.polis.dao.shakhmin;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.jetbrains.annotations.NotNull;

public final class Row implements Comparable<Row> {

    @NotNull private final ByteBuffer key;
    @NotNull private final Value value;
    private final long serialNumber;

    private static final Comparator<Row> COMPARATOR =
            Comparator
                    .comparing(Row::getKey)
                    .thenComparing(Row::getValue)
                    .thenComparing((r) -> -r.getSerialNumber());

    private Row(
            @NotNull final ByteBuffer key,
            @NotNull final Value value,
            final long serialNumber) {
        this.key = key;
        this.value = value;
        this.serialNumber = serialNumber;
    }

    public static Row of(
            @NotNull final ByteBuffer key,
            @NotNull final Value value,
            final long serialNumber) {
        return new Row(key, value, serialNumber);
    }

    @NotNull
    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    @NotNull
    public Value getValue() {
        return value;
    }

    public long getSerialNumber() {
        return serialNumber;
    }

    public static long getSizeOfFlushedRow(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        return Integer.BYTES + key.remaining() + Long.BYTES
                + (value.remaining() == 0 ? 0 : Long.BYTES + value.remaining());
    }

    @Override
    public int compareTo(@NotNull final Row row) {
        return COMPARATOR.compare(this, row);
    }
}
