package ru.mail.polis.dao.shakhmin;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.jetbrains.annotations.NotNull;

public final class Row implements Comparable<Row> {

    @NotNull private final ByteBuffer key;
    @NotNull private final Cell cell;
    private final long serialNumber;

    private static final Comparator<Row> COMPARATOR =
            Comparator
                    .comparing(Row::getKey)
                    .thenComparing(Row::getCell)
                    .thenComparing((r) -> -r.getSerialNumber());

    private Row(
            @NotNull final ByteBuffer key,
            @NotNull final Cell cell,
            final long serialNumber) {
        this.key = key;
        this.cell = cell;
        this.serialNumber = serialNumber;
    }

    public static Row of(
            @NotNull final ByteBuffer key,
            @NotNull final Cell cell,
            final long serialNumber) {
        return new Row(key, cell, serialNumber);
    }

    @NotNull
    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    @NotNull
    public Cell getCell() {
        return cell;
    }

    public long getSerialNumber() {
        return serialNumber;
    }

    public static long getSizeOfFlushedRow(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        return ((long) Integer.BYTES) + key.remaining() + Long.BYTES
                + (value.remaining() == 0 ? 0 : Long.BYTES + value.remaining());
    }

    @Override
    public int compareTo(@NotNull final Row row) {
        return COMPARATOR.compare(this, row);
    }
}
