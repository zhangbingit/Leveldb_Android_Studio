package com.bb.leveldb;

public class DatabaseCorruptException extends LevelDBException {
    private static final long serialVersionUID = 1L;

    public DatabaseCorruptException() {
    }

    public DatabaseCorruptException(String error) {
        super(error);
    }
}
