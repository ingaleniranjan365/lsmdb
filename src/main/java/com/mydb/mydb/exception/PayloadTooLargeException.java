package com.mydb.mydb.exception;

public class PayloadTooLargeException extends Exception {
  public PayloadTooLargeException(String errorMessage) {
    super(errorMessage);
  }
}
