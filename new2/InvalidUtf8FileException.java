package com.test.dataflowengine.utils;

import java.io.IOException;

public class InvalidUtf8FileException extends IOException {

    public InvalidUtf8FileException(String message) {
        super(message);
    }

    public InvalidUtf8FileException(String message, Throwable cause) {
        super(message, cause);
    }
}
