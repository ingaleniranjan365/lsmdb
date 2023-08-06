package com.lsmdb.exception;

public class PayloadTooLargeException extends RuntimeException {
        public PayloadTooLargeException(String errorMessage) {
                super(errorMessage);
        }
}