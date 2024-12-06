package com.nlu.app.exception;

import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

import java.sql.SQLException;

public class GlobalHandlerException {
    public static void handleException(SQLException e) {
        var errorCode = Integer.parseInt(e.getSQLState());
        var processException = ProcessExceptionEnum.getBasedOnCode(errorCode);
        if (processException != null) {
            System.out.println(processException.getMessage());
        }
    }

    public static void handleGlobalException(Exception e) {
        if (e instanceof UnableToExecuteStatementException) {
            var casted = (UnableToExecuteStatementException) e;
            if (casted.getCause() instanceof SQLException) {
                handleException((SQLException) casted.getCause());
            }
        }
    }
}
