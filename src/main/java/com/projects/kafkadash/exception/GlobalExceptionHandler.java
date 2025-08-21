package com.projects.kafkadash.exception;

import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler {

    Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

//    @ExceptionHandler(value = UnknownTopicOrPartitionException.class)
//    public void handleAllExceptions(UnknownTopicOrPartitionException ex){
//        logger.error("Topic: {} doesn't exist on kafka.", ex);
//        logger.error(ex.getMessage(), ex);
//    }

    @ExceptionHandler(value = Exception.class)
    public void handleAllExceptions(Exception ex){
        logger.error(ex.getMessage(), ex);
    }
}
