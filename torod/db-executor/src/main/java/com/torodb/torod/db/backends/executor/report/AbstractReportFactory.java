
package com.torodb.torod.db.backends.executor.report;

import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.db.backends.executor.jobs.*;

import com.torodb.torod.db.executor.jobs.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AbstractReportFactory implements ReportFactory {
    private static final DummyReport DUMMY_REPORT = DummyReport.INSTANCE;

    @Override
    public CloseConnectionCallable.Report createCloseConnectionReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CloseCursorCallable.Report createCloseCursorReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CommitCallable.Report createCommitReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CreateCollectionCallable.Report createCreateCollectionReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CreateSubDocTableCallable.Report createCreateSubDocTableReport() {
        return DUMMY_REPORT;
    }

    @Override
    public DeleteCallable.Report createDeleteReport() {
        return DUMMY_REPORT;
    }

    @Override
    public FindCollectionsCallable.Report createFindCollectionsReport() {
        return DUMMY_REPORT;
    }

    @Override
    public InsertCallable.Report createInsertReport() {
        return DUMMY_REPORT;
    }

    @Override
    public QueryCallable.Report createQueryReport() {
        return DUMMY_REPORT;
    }

    @Override
    public ReadAllCursorCallable.Report createReadAllCursorReport() {
        return DUMMY_REPORT;
    }

    @Override
    public ReadCursorCallable.Report createReadCursorReport() {
        return DUMMY_REPORT;
    }

    @Override
    public ReserveSubDocIdsCallable.Report createReserveSubDocIdsReport() {
        return DUMMY_REPORT;
    }

    @Override
    public RollbackCallable.Report createRollbackReport() {
        return DUMMY_REPORT;
    }

    @Override
    public DropCollectionCallable.Report createDropCollectionReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CreateIndexCallable.Report createIndexReport() {
        return DUMMY_REPORT;
    }

    @Override
    public DropIndexCallable.Report createDropIndexReport() {
        return DUMMY_REPORT;
    }

    @Override
    public GetIndexesCallable.Report createGetIndexReport() {
        return DUMMY_REPORT;
    }

    @Override
    public GetDatabasesCallable.Report createGetDatabasesReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CountCallable.Report createCountReport() {
        return DUMMY_REPORT;
    }

    @Override
    public GetIndexSizeCallable.Report createGetIndexSizeReport() {
        return DUMMY_REPORT;
    }

    @Override
    public GetCollectionSizeCallable.Report createGetCollectionSizeReport() {
        return DUMMY_REPORT;
    }

    @Override
    public GetDocumentsSize.Report createGetDocumentsSizeReport() {
        return DUMMY_REPORT;
    }

    @Override
    public GetCollectionsMetainfoCallable.Report createGetCollectionsMetainfoReport() {
        return DUMMY_REPORT;
    }

    @Override
    public MaxElementsCallable.Report createMaxElementsReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CreatePathViewsCallable.Report createCreatePathViewsReport() {
        return DUMMY_REPORT;
    }

    @Override
    public DropPathViewsCallable.Report createDropPathViewsReport() {
        return DUMMY_REPORT;
    }

    @Override
    public SqlSelectCallable.Report createSqlSelectReport() {
        return DUMMY_REPORT;
    }

    private static class DummyReport implements CloseConnectionCallable.Report,
            CloseCursorCallable.Report, CommitCallable.Report,
            CreateCollectionCallable.Report, 
            CreateSubDocTableCallable.Report, DeleteCallable.Report,
            FindCollectionsCallable.Report, InsertCallable.Report,
            QueryCallable.Report, ReadAllCursorCallable.Report, 
            ReadCursorCallable.Report, ReserveSubDocIdsCallable.Report,
            RollbackCallable.Report, DropCollectionCallable.Report, 
            CreateIndexCallable.Report, DropIndexCallable.Report,
            GetIndexesCallable.Report, GetDatabasesCallable.Report,
            CountCallable.Report, GetIndexSizeCallable.Report, 
            GetCollectionSizeCallable.Report, GetDocumentsSize.Report, 
            GetCollectionsMetainfoCallable.Report, MaxElementsCallable.Report,
            CreatePathViewsCallable.Report, DropPathViewsCallable.Report,
            SqlSelectCallable.Report {

        public static final DummyReport INSTANCE = new DummyReport();

        @Override
        public void closeConnectionExecuted() {
        }

        @Override
        public void closeCursorExecuted(CursorId cursorId) {
        }

        @Override
        public void commitExecuted() {
        }

        @Override
        public void createCollectionExecuted(String collection) {
        }

        @Override
        public void createSubDocTableExecuted(String collection, SubDocType type) {
        }

        @Override
        public void deleteExecuted(String collection, List<? extends DeleteOperation> deletes, WriteFailMode mode) {
        }

        @Override
        public void findCollectionExecuted(Map<String, Integer> collections) {
        }

        @Override
        public void insertExecuted(String collection, Collection<SplitDocument> docs, WriteFailMode mode, InsertResponse response) {
        }

        @Override
        public void queryExecuted(String collection, CursorId cursorId, QueryCriteria filter, Projection projection, int maxResults) {
        }

        @Override
        public void readAllCursorExecuted(CursorId cursorId, List<? extends SplitDocument> result) {
        }

        @Override
        public void readCursorExecuted(CursorId cursorId, int maxResult, List<SplitDocument> result) {
        }

        @Override
        public void reserveSubDocIdsExecuted(String collection, int reservedIds) {
        }

        @Override
        public void rollbackExecuted() {
        }

        @Override
        public void dropCollectionExecuted(String collection) {
        }

        @Override
        public void createIndexExecuted(
                String collectionName, 
                String indexName,
                IndexedAttributes attributes,
                boolean unique,
                boolean blocking,
                NamedToroIndex result) {
        }

        @Override
        public void dropIndexExecuted(String collection, String indexName, boolean removed) {
        }

        @Override
        public void getIndexesExecuted(String collection, Collection<? extends NamedToroIndex> result) {
        }

        @Override
        public void getDatabasesExecuted(List<? extends Database> databases) {
        }

        @Override
        public void countExecuted(String collection, QueryCriteria query, int count) {
        }

        @Override
        public void getIndexSizeExecuted(String collection, String index, Long size) {
        }

        @Override
        public void getCollectionSizeExecuted(String collection, Long size) {
        }

        @Override
        public void getDocumentSizeExecuted(String collection, Long size) {
        }

        @Override
        public void getCollectionsMetainfoExecuted(List<CollectionMetainfo> metainfo) {
        }

        @Override
        public void maxElementsExecuted(CursorId cursorId, int result) {
        }

        @Override
        public void createViewsExecuted(String collection, Integer result) {
        }

        @Override
        public void dropViewsExecuted(String collection) {
        }

        @Override
        public void sqlSelectExecuted(String query) {
        }

    }

}
