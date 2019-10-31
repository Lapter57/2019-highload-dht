package ru.mail.polis.dao.shakhmin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

public final class SSTable implements Table {

    @NotNull private final LongBuffer offsets;
    @NotNull private final ByteBuffer rows;
    private final long rowsNumber;
    private final long serialNumber;
    private final long sizeInBytes;

    /**
     * Constructs a new SSTable.
     *
     * @param path the path of the file where data of SSTable is stored
     * @param serialNumber the serial number of SStable
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if serial number less than 0
     */
    public SSTable(
            @NotNull final Path path,
            final long serialNumber) throws IOException, IllegalArgumentException {
        if (serialNumber < 0) {
            throw new IllegalArgumentException("Serial number must not be less than 0");
        }
        this.serialNumber = serialNumber;
        try (var fc = FileChannel.open(path, StandardOpenOption.READ)) {
            this.sizeInBytes = fc.size();
            final var mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size())
                    .order(ByteOrder.BIG_ENDIAN);
            this.rowsNumber = mapped.getLong(mapped.limit() - Long.BYTES);

            final var offsetsBuffer = mapped.duplicate()
                    .position((int) (mapped.limit() - Long.BYTES - Long.BYTES * rowsNumber))
                    .limit(mapped.limit() - Long.BYTES);
            this.offsets = offsetsBuffer.slice().asLongBuffer();

            this.rows = mapped.asReadOnlyBuffer()
                    .limit(offsetsBuffer.position())
                    .slice();
        }
    }

    @NotNull
    private ByteBuffer rowAt(final long offsetPosition) {
        final long offset = offsets.get((int) offsetPosition);
        final long rowSize = (offsetPosition == rowsNumber - 1)
                ? rows.limit() - offset
                : offsets.get((int) (offsetPosition + 1)) - offset;
        return rows.asReadOnlyBuffer()
                .position((int) offset)
                .limit((int) (rowSize + offset))
                .slice();
    }

    @NotNull
    private ByteBuffer keyAt(@NotNull final ByteBuffer row) {
        final var rowBuffer = row.asReadOnlyBuffer();
        final int keySize = rowBuffer.getInt();
        return rowBuffer.limit(keySize + Integer.BYTES)
                .slice();
    }

    private long timestampAt(@NotNull final ByteBuffer row) {
        final var rowBuffer = row.asReadOnlyBuffer();
        final int keySize = rowBuffer.getInt();
        return rowBuffer.position(keySize + Integer.BYTES)
                .getLong();
    }

    @NotNull
    private ByteBuffer valueAt(@NotNull final ByteBuffer row) {
        final var rowBuffer = row.asReadOnlyBuffer();
        final int keySize = rowBuffer.getInt();
        return rowBuffer.position(keySize + Integer.BYTES + Long.BYTES * 2)
                .slice();
    }

    @NotNull
    private Row transform(final long offsetPosition) {
        final var row = rowAt(offsetPosition);
        final var key = keyAt(row);
        final var timestamp = timestampAt(row);
        final var value = timestamp < 0
                ? Cell.tombstone(-timestamp)
                : Cell.of(timestamp, valueAt(row));
        return Row.of(key, value, serialNumber);
    }

    private long position(@NotNull final ByteBuffer key) {
        long left = 0;
        long right = rowsNumber - 1;
        while(left <= right) {
            final long mid = (left + right) >>> 1;
            final int cmp = keyAt(rowAt(mid)).compareTo(key);
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new SSTableIterator(from);
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public long serialNumber() {
        return serialNumber;
    }

    private class SSTableIterator implements Iterator<Row> {
        private long position;

        SSTableIterator(@NotNull final ByteBuffer from) {
            this.position = position(from);
        }

        @Override
        public boolean hasNext() {
            return position < rowsNumber;
        }

        @Override
        public Row next() {
            if(!hasNext()){
                throw new NoSuchElementException();
            }
            final var row = transform(position);
            position++;
            return row;
        }
    }

    /**
     * Flush of data to disk as SSTable.
     *
     * @param flushedFile the path of the file to which the data is flushed
     * @param rowsIterator the iterator to flush rows ({@link Row}
     * @throws IOException if an I/O error occurs
     */
    public static void flush(
            @NotNull final Path flushedFile,
            @NotNull final Iterator<Row> rowsIterator) throws IOException {
        long offset = 0L;
        final var offsets = new ArrayList<Long>();
        offsets.add(offset);
        try (var fc = FileChannel.open(
                flushedFile,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            while (rowsIterator.hasNext()) {
                final var row = rowsIterator.next();
                final var key = row.getKey();
                final var cell = row.getCell();
                final var sizeRow = Row.getSizeOfFlushedRow(key, cell.getData());
                final var rowBuffer = ByteBuffer.allocate((int) sizeRow)
                        .putInt(key.remaining())
                        .put(key.duplicate())
                        .putLong(cell.getTimestamp());
                if (!cell.isRemoved() && cell.getData().remaining() != 0) {
                    final var data = cell.getData();
                    rowBuffer.putLong(data.remaining())
                            .put(data.duplicate());
                }
                offset += sizeRow;
                offsets.add(offset);
                rowBuffer.flip();
                fc.write(rowBuffer);
            }
            offsets.remove(offsets.size() - 1);
            final var offsetsBuffer = ByteBuffer.allocate(
                    offsets.size() * Long.BYTES + Long.BYTES);
            for (final var anOffset: offsets) {
                offsetsBuffer.putLong(anOffset);
            }
            offsetsBuffer.putLong(offsets.size())
                    .flip();
            fc.write(offsetsBuffer);
        }
    }
}
