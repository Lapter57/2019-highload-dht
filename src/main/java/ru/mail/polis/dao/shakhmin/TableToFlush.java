package ru.mail.polis.dao.shakhmin;

import org.jetbrains.annotations.NotNull;

public class TableToFlush {
    @NotNull private final Table table;
    private final long serialNumber;
    private final boolean poisonPill;

    public TableToFlush(final long serialNumber,
                        @NotNull Table table) {
        this(serialNumber, table, false);
    }

    public TableToFlush(final long serialNumber,
                        @NotNull final Table table,
                        final boolean poisonPill) {
        this.serialNumber = serialNumber;
        this.table = table;
        this.poisonPill = poisonPill;
    }

    public long getSerialNumber() {
        return serialNumber;
    }

    @NotNull
    public Table getTable() {
        return table;
    }

    public boolean isPoisonPill() {
        return poisonPill;
    }
}
