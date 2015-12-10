package com.torodb.torod.db.backends.executor.report;

import com.torodb.torod.db.backends.executor.jobs.*;

/**
 *
 */
public interface ReportFactory {

    public CloseConnectionCallable.Report createCloseConnectionReport();

    public CloseCursorCallable.Report createCloseCursorReport();

    public CommitCallable.Report createCommitReport();

    public CreateCollectionCallable.Report createCreateCollectionReport();

    public CreateSubDocTableCallable.Report createCreateSubDocTableReport();

    public DeleteCallable.Report createDeleteReport();

    public FindCollectionsCallable.Report createFindCollectionsReport();

    public InsertCallable.Report createInsertReport();

    public QueryCallable.Report createQueryReport();

    public ReadAllCursorCallable.Report createReadAllCursorReport();

    public ReadCursorCallable.Report createReadCursorReport();

    public ReserveSubDocIdsCallable.Report createReserveSubDocIdsReport();

    public RollbackCallable.Report createRollbackReport();

    public DropCollectionCallable.Report createDropCollectionReport();

    public CreateIndexCallable.Report createIndexReport();

    public DropIndexCallable.Report createDropIndexReport();

    public GetIndexesCallable.Report createGetIndexReport();

    public GetDatabasesCallable.Report createGetDatabasesReport();

    public CountCallable.Report createCountReport();

    public GetIndexSizeCallable.Report createGetIndexSizeReport();

    public GetCollectionSizeCallable.Report createGetCollectionSizeReport();

    public GetDocumentsSize.Report createGetDocumentsSizeReport();
    
    public GetCollectionsMetainfoCallable.Report createGetCollectionsMetainfoReport();

    public MaxElementsCallable.Report createMaxElementsReport();
}
