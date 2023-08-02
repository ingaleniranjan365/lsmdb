package com.lsmdb.exception;

public class ElementNotFoundException extends RuntimeException {
  public ElementNotFoundException(String errorMessage) {
    super(errorMessage);
  }
}

