package com.ytfs.service.dao;

public class ProxyException extends RuntimeException {

    public ProxyException() {
        super();
    }

    public ProxyException(String message) {
        super(message);
    }

    public ProxyException(Throwable cause) {
        super(cause);
    }
}
