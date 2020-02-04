package jp.co.dwango.ddex.report;

import com.google.inject.ImplementedBy;

@ImplementedBy(ReportBatchImpl.class)
public interface ReportBatch {
    public void execute(String[] args);
}
