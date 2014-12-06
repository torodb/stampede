package com.torodb.torod.db.executor.report;

/**
 *
 */
public interface ReserveSubDocIdsReport {

    public void taskExecuted(String collection, int reservedIds);
}
