package ru.mail.polis.dao.shakhmin;

import java.util.NoSuchElementException;

public class NoSuchElementLiteException extends NoSuchElementException {
    public NoSuchElementLiteException(final String s) {
        super(s);
    }

    @Override
    public Throwable fillInStackTrace() {
        synchronized(NoSuchElementLiteException.class){
            return this;
        }
    }
}
