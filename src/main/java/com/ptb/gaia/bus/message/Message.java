package com.ptb.gaia.bus.message;

import java.util.Arrays;

/**
 * Created by eric on 16/1/11.
 */
public class Message<T> {
    String src;
    String dest;
    int type;
    String version;
    Long timestamp;
    T body;

    public transient byte[] raw;


    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Message() {
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getRaw() {
        return raw;
    }

    public void setRaw(byte[] raw) {
        this.raw = raw;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }


    public T getBody() {
        return body;
    }

    public void setBody(T body) {
        this.body = body;
    }

    public Message(String src, String dest, int type, String version, Long timestamp, T body) {
        this.src = src;
        this.dest = dest;
        this.type = type;
        this.version = version;
        this.timestamp = timestamp;
        this.body = body;
    }

    public Message(String src, String dest, int type, String version, T body) {
        this.src = src;
        this.dest = dest;
        this.type = type;
        this.version = version;
        this.timestamp = System.currentTimeMillis();
        this.body = body;
    }


    public Message(byte[] raw) {
        this.raw = raw;
    }

    @Override
    public String toString() {
        return "Message{" +
                "src='" + src + '\'' +
                ", dest='" + dest + '\'' +
                ", type='" + type + '\'' +
                ", version='" + version + '\'' +
                ", timestamp=" + timestamp +
                ", body=" + body +
                ", raw=" + Arrays.toString(raw) +
                '}';
    }
}
