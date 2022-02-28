package com.mydb.mydb.exception;

public class UnknownProbeException extends RuntimeException {
  public UnknownProbeException(String errorMessage) {
    super(errorMessage);
  }
}
