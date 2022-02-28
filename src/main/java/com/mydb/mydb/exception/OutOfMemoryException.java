package com.mydb.mydb.exception;

public class OutOfMemoryException extends RuntimeException {
  public OutOfMemoryException(String errorMessage) {
    super(errorMessage);
  }
}
