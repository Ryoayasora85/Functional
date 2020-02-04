package jp.co.dwango.ddex.report;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nonnull;
import jp.co.dwango.ddex.db.report.entity.Report;

public interface ReportFileBuilder extends Closeable {
    @Nonnull ReportFileBuilder add(@Nonnull Report report) throws IOException;
    void build() throws IOException;
}
