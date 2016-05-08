package com.bb.leveldb;

public class LevelDBException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public LevelDBException() {
    }

    public LevelDBException(String error) {
        super(error);
    }

    public LevelDBException(String error, Throwable cause) {
        super(error, cause);
    }
}
