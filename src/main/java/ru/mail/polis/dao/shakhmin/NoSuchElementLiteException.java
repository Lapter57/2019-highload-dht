package ru.mail.polis.dao.shakhmin;

import java.util.NoSuchElementException;

public class NoSuchElementLiteException extends NoSuchElementException {
    private static final long serialVersionUID = 0L;

    public NoSuchElementLiteException(final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
