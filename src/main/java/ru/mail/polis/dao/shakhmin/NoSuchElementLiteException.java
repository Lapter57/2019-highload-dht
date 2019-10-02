package ru.mail.polis.dao.shakhmin;

import java.util.NoSuchElementException;

public class NoSuchElementLiteException extends NoSuchElementException {
    public NoSuchElementLiteException(String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
