package com.ptb.gaia.bus;

public class BusException extends RuntimeException {
    private long date = System.currentTimeMillis();

    public long getDate() {
        return this.date;
    }

    public BusException(String message) {
        super(message);
    }
}
