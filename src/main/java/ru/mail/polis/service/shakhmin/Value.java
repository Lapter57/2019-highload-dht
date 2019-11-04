package ru.mail.polis.service.shakhmin;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.dao.shakhmin.Cell;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Collection;

final class Value implements Comparable<Value> {
    private static final String TIMESTAMP_HEADER_NAME = "X-OK-Timestamp";
    private static final Value ABSENT = new Value(null, -1, State.ABSENT);

    @Nullable private final byte[] data;
    private final long timestamp;
    @NotNull private final State state;

    private Value(@Nullable final byte[] data,
                  final long timestamp,
                  @NotNull final State state) {
        this.data = data == null ? null : Arrays.copyOf(data, data.length);
        this.timestamp = timestamp;
        this.state = state;
    }

    @NotNull
    static Value present(@NotNull final byte[] data,
                         final long timestamp) {
        return new Value(data, timestamp, State.PRESENT);
    }

    @NotNull
    static Value removed(final long timestamp) {
        return new Value(null, Math.abs(timestamp), State.REMOVED);
    }

    @NotNull
    static Value absent() {
        return ABSENT;
    }

    @NotNull
    public static Value from(@NotNull final HttpResponse<byte[]> response) {
        final var headers = response.headers();
        final var timestamp = headers.firstValue(TIMESTAMP_HEADER_NAME.toLowerCase()).orElse(null);
        if (response.statusCode() == 200) {
            if (timestamp == null) {
                throw new IllegalArgumentException("Wrong input data");
            }
            return present(response.body(), Long.parseLong(timestamp));
        } else if (response.statusCode() == 404) {
            if (timestamp == null) {
                return absent();
            } else {
                return removed(Long.parseLong(timestamp));
            }
        } else {
            throw new IllegalArgumentException("Bad response");
        }
    }

    @NotNull
    public static Value from(@Nullable final Cell cell) {
        if (cell == null) {
            return Value.absent();
        }
        if (cell.isRemoved()) {
            return Value.removed(cell.getTimestamp());
        } else {
            final var data = cell.getData();
            final var buffer = new byte[data.remaining()];
            data.duplicate().get(buffer);
            return Value.present(buffer, cell.getTimestamp());
        }
    }

    @NotNull
    public static Response transform(@NotNull final Value value,
                                     final boolean proxied) {
        Response result;
        switch (value.getState()) {
            case PRESENT:
                result = new Response(Response.OK, value.getData());
                if (proxied) {
                    result.addHeader(TIMESTAMP_HEADER_NAME + ": " + value.getTimestamp());
                }
                return result;
            case REMOVED:
                result = new Response(Response.NOT_FOUND, Response.EMPTY);
                if (proxied) {
                    result.addHeader(TIMESTAMP_HEADER_NAME + ": " + value.getTimestamp());
                }
                return result;
            case ABSENT:
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            default:
                throw new IllegalArgumentException("Wrong input data");
        }
    }

    static Value merge(@NotNull final Collection<Value> values) {
        return values.stream()
                .filter(v -> v.getState() != State.ABSENT)
                .max(Value::compareTo)
                .orElseGet(Value::absent);
    }

    @Nullable
    byte[] getData() {
        return data == null
                ? null
                : Arrays.copyOf(data, data.length);
    }

    long getTimestamp() {
        return timestamp;
    }

    @NotNull
    State getState() {
        return state;
    }

    @Override
    public int compareTo(@NotNull final Value value) {
        return Long.compare(Math.abs(timestamp), Math.abs(value.timestamp));
    }

    private enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}
