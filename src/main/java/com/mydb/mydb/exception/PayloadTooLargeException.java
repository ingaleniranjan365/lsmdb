package com.mydb.mydb.exception;


import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.BAD_REQUEST, reason="Payload size not within expected limits")
public class PayloadTooLargeException extends RuntimeException {}
