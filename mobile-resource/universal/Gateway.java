package jp.co.dwango.ddex.report;

import jp.co.dwango.ddex.report.ReportBatch;

import com.google.inject.Guice;

/**
 * バッチのゲートウェイ
 */
public class Gateway {
    public static void main(String[] args) {
        Guice.createInjector(
            new ReportModule()
        ).getInstance(ReportBatch.class).execute(args);
    }
}
