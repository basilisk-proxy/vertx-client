package com.milestone.basilisk.vertx;

@SuppressWarnings("unused")
public final class ProtocolTypes {
    public static final String CONNECT = "connect";
    public static final String SUBSCRIBE = "subscribe";
    public static final String UNSUBSCRIBE = "unsubscribe";
    public static final String PUBLISH = "publish";
    public static final String FORWARD = "forward";
    public static final String FORWARD_RESPONSE = "forward_response";
    public static final String EVENT = "event";
    public static final String ACK = "ack";
    public static final String ERROR = "error";

    private ProtocolTypes() {
    }
}
