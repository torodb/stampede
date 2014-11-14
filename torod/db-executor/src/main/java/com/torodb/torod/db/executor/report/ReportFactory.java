package com.torodb.torod.db.executor.report;

/**
 *
 */
public interface ReportFactory {

    public CloseConnectionReport createCloseConnectionReport();

    public CloseCursorReport createCloseCursorReport();

    public CommitReport createCommitReport();

    public CountRemainingDocsReport createCountRemainingDocsReport();

    public CreateCollectionReport createCreateCollectionReport();

    public CreateSubDocTableReport createCreateSubDocTableReport();

    public DeleteReport createDeleteReport();

    public FindCollectionsReport createFindCollectionsReport();

    public InsertReport createInsertReport();

    public QueryReport createQueryReport();

    public ReadAllCursorReport createReadAllCursorReport();

    public ReadCursorReport createReadCursorReport();

    public ReserveSubDocIdsReport createReserveSubDocIdsReport();

    public RollbackReport createRollbackReport();


}
