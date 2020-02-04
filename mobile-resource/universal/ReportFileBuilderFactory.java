package jp.co.dwango.ddex.report;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.inject.ImplementedBy;

@ImplementedBy(ReportFileBuilderFactoryImpl.class)
public interface ReportFileBuilderFactory {
    @Nonnull ReportFileBuilder create(@Nonnull File outputPath, boolean isHeaderRequired) throws IOException, FileNotFoundException;
}
