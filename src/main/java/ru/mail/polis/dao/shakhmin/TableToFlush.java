package ru.mail.polis.dao.shakhmin;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class TableToFlush {
    @NotNull private final Iterator<Row> rows;
    private final long serialNumber;
    private final boolean poisonPill;
    private final boolean isCompactTable;

    public static class Builder {
        @NotNull private final Iterator<Row> rows;
        private final long serialNumber;

        private boolean poisonPill = false;
        private boolean isCompactTable = false;

        public Builder(@NotNull Iterator<Row> rows,
                       final long serialNumber) {
            this.rows = rows;
            this.serialNumber = serialNumber;
        }

        public Builder poisonPill() {
            poisonPill = true;
            return this;
        }

        public Builder isCompactTable() {
            isCompactTable = true;
            return this;
        }

        public TableToFlush build() {
            return new TableToFlush(this);
        }
    }

    private TableToFlush(@NotNull final Builder builder) {
        this.serialNumber = builder.serialNumber;
        this.rows = builder.rows;
        this.poisonPill = builder.poisonPill;
        this.isCompactTable = builder.isCompactTable;
    }

    private TableToFlush(@NotNull Iterator<Row> rows,
                         final long serialNumber) {
        this.serialNumber = serialNumber;
        this.rows = rows;
        this.poisonPill = false;
        this.isCompactTable = false;
    }

    public static TableToFlush of(@NotNull Iterator<Row> rows,
                                  final long serialNumber) {
        return new TableToFlush(rows, serialNumber);
    }

    public long getSerialNumber() {
        return serialNumber;
    }

    @NotNull
    public Iterator<Row> getTable() {
        return rows;
    }

    public boolean isPoisonPill() {
        return poisonPill;
    }

    public boolean isCompactTable() {
        return isCompactTable;
    }
}
