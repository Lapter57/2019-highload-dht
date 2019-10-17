package ru.mail.polis.dao.shakhmin;

import java.util.NoSuchElementException;

public class NoSuchElementLiteException extends NoSuchElementException {
    private static final long serialVersionUID = 0L;

    public NoSuchElementLiteException(final String s) {
        super(s);
    }

    @Override
    @SuppressWarnings("UnsynchronizedOverridesSynchronized") // synchronized on the monitor object
    public Throwable fillInStackTrace() {
        synchronized(NoSuchElementLiteException.class){
            return this;
        }
    }
}
