
package com.torodb.torod.db.executor.report;

import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.SubDocType;
import java.util.List;

/**
 *
 */
public class AbstractReportFactory implements ReportFactory {
    private static final DummyReport DUMMY_REPORT = DummyReport.INSTANCE;

    @Override
    public CloseConnectionReport createCloseConnectionReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CloseCursorReport createCloseCursorReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CommitReport createCommitReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CountRemainingDocsReport createCountRemainingDocsReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CreateCollectionReport createCreateCollectionReport() {
        return DUMMY_REPORT;
    }

    @Override
    public CreateSubDocTableReport createCreateSubDocTableReport() {
        return DUMMY_REPORT;
    }

    @Override
    public DeleteReport createDeleteReport() {
        return DUMMY_REPORT;
    }

    @Override
    public FindCollectionsReport createFindCollectionsReport() {
        return DUMMY_REPORT;
    }

    @Override
    public InsertReport createInsertReport() {
        return DUMMY_REPORT;
    }

    @Override
    public QueryReport createQueryReport() {
        return DUMMY_REPORT;
    }

    @Override
    public ReadAllCursorReport createReadAllCursorReport() {
        return DUMMY_REPORT;
    }

    @Override
    public ReadCursorReport createReadCursorReport() {
        return DUMMY_REPORT;
    }

    @Override
    public ReserveSubDocIdsReport createReserveSubDocIdsReport() {
        return DUMMY_REPORT;
    }

    @Override
    public RollbackReport createRollbackReport() {
        return DUMMY_REPORT;
    }

    private static class DummyReport implements CloseConnectionReport,
            CloseCursorReport, CommitReport, CountRemainingDocsReport,
            CreateCollectionReport, CreateSubDocTableReport, DeleteReport,
            FindCollectionsReport, InsertReport, QueryReport,
            ReadAllCursorReport, ReadCursorReport, ReserveSubDocIdsReport,
            RollbackReport {

        public static final DummyReport INSTANCE = new DummyReport();

        @Override
        public void taskExecuted() {
        }

        @Override
        public void taskExecuted(CursorId cursorId) {
        }

        @Override
        public void taskExecuted(CursorId cursorId, int remainingDocs) {
        }

        @Override
        public void taskExecuted(String collection) {
        }

        @Override
        public void taskExecuted(String collection, SubDocType type) {
        }

        @Override
        public void taskExecuted(String collection, List<? extends DeleteOperation> deletes, WriteFailMode mode) {
        }

        @Override
        public void taskExecuted(int insertedDocCount) {
        }

        @Override
        public void taskExecuted(String collection, CursorId cursorId, QueryCriteria filter, Projection projection, int maxResults, int realResultCount) {
        }

        @Override
        public void tastExecuted(CursorId cursorId) {
        }

        @Override
        public void taskExecuted(String collection, int reservedIds) {
        }
    }

}
