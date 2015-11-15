
package com.torodb.torod.db.wrappers.executor.report;

/**
 *
 */
public class DummyReportFactory extends AbstractReportFactory {
    private static final DummyReportFactory INSTANCE = new DummyReportFactory();

    private DummyReportFactory() {
    }

    public static ReportFactory getInstance() {
        return INSTANCE;
    }

}
