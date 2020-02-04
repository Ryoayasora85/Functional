package jp.co.dwango.ddex.report;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.mail.MessagingException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.co.dwango.ddex.core.entity.EntityProcessor;
import jp.co.dwango.ddex.db.report.entity.Report;
import jp.co.dwango.ddex.db.report.entity.ReportForAndroid;
import jp.co.dwango.ddex.db.report.entity.ReportForAndroidBundle;
import jp.co.dwango.ddex.db.report.service.ReportService;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * ユニバーサル向けレポート作成
 *
 */
public class ReportBatchImpl implements ReportBatch {
    private boolean error;
    private ReportMail mailer;

    private final Logger logger = LoggerFactory.getLogger(ReportBatchImpl.class);
    private final ReportService reportService;
    private final ReportFileBuilderFactory reportFileWriterFactory;
    private final String reportPutDirPath;
    private final String reportGetDirPath;

    private static final int MAX_FILE_COUNT = 6; // 過去ファイルの最大保存数
    private static final String FEATURE_FILE_NAME = "new_mobile_out.txt";
    private static final String ANDROID_FILE_NAME = "umgi-meta.tsv";
    private static final String ANDROID_BUNDLE_FILE_NAME = "umgi_meta-b.tsv";
    static final String[] FORMAT ={
        "partnerSeq",
        "partnerName",
        "beginDate",
        "itemCategory",
        "productType",
        "grId",
        "icpn",
        "isrc",
        "usageType",
        "salesPrice",
        "priceId",
        "title",
        "titleYomi",
        "titleEng",
        "artist",
        "artistYomi",
        "artistEng",
        "musicIsrc",
        "musicTitle",
        "packageCode",
        "packageUpc",
        "packageTitle",
        "packageTitleEng",
        "packageArtist",
        "packageArtistEng",
        "copyrightId",
        "copyrightCode",
        "copyrightTitle",
        "copyrightSongwriter",
        "copyrightTranslater",
        "copyrightComposer",
        "copyrightArranger",
        "copyrightArtist",
        "artistCode",
        "priceTaxIn",
        "priceTaxOut",
        "releaseDate"
    };

    private static final Pattern DEFAULT_REPORT_FILE_NAME_PATTERN = Pattern.compile("^(\\d{12})_\\d+_\\d{4}-\\d{2}-\\d{2}_\\d{4}-\\d{2}-\\d{2}\\.txt$");
    private static final Pattern ADD_REPORT_FILE_NAME_PATTERN = Pattern.compile("^(\\d{12})_\\d+_Add\\.txt$");
    private static final Pattern CHANGE_REPORT_FILE_NAME_PATTERN = Pattern.compile("^@LW\\d+_(\\d{4})-(\\d{2})-(\\d{2})_(\\d{2})-(\\d{2})-\\d{2}\\.xls$");

    // ユニバーサル側の都合で, 「受け取るデータに書いてある価格コードと, 売上げレポートに出力するべき価格コードに乖離がある」ため,
    // レポートに出力する価格コードについて, このMapを使用して, 先方の指定通りに変換する.
    // ・このMap上に無い組合せ(=イレギュラー価格)に関しては、価格コードは空欄にする
    // ・きちんとやるなら, 組合せ情報をDBに格納しておくべきだろうが,
    //   もはやガラケー素材の素材種なんか増えないだろう, とたかをくくってハードコードしてある
    private static final Map<Pair<String, Integer>, String> UNIFIED_PRICE_CODE_BY_ITEM_CATEGORY_AND_SALES_PRICE = new ImmutableMap.Builder<Pair<String, Integer>, String>()
            .put(Pair.of("フルムービー", 300), "MAP")
            .put(Pair.of("フルムービー", 400), "TAP")
            .put(Pair.of("フルムービー", 500), "STAP")
            .put(Pair.of("歌詞付き着うたフル", 300), "TAP")
            .put(Pair.of("歌詞付き着うたフル", 400), "STAP")
            .put(Pair.of("着ヴォイス", 100), "TAP")
            .put(Pair.of("着ヴォイス＆ＢＧＭ", 100), "TAP")
            .put(Pair.of("着うた", 100), "TAP")
            .put(Pair.of("着うた", 200), "STAP")
            .put(Pair.of("着うたフル", 300), "TAP")
            .put(Pair.of("着うたフル", 400), "STAP")
            .put(Pair.of("着うたフル配信", 300), "TAP")
            .put(Pair.of("着うたフル配信", 400), "STAP")
            .put(Pair.of("着うた配信", 100), "TAP")
            .put(Pair.of("着うた配信", 200), "STAP")
            .put(Pair.of("着ムービー", 200), "TAP")
            .put(Pair.of("着ムービー", 300), "STAP")
            .build();

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

    @Inject
    public ReportBatchImpl(
        ReportService reportService,
        ReportFileBuilderFactory reportFileWriterFactory,
        @Named("reportPutDirPath") String reportPutDirPath,
        @Named("reportGetDirPath") String reportGetDirPath,
        ReportMail reportMail
    ) {
        this.reportService = reportService;
        this.reportFileWriterFactory = reportFileWriterFactory;
        this.reportPutDirPath = reportPutDirPath;
        this.reportGetDirPath = reportGetDirPath;
        this.mailer = reportMail;
        this.error = false;
    }

    public void execute(String[] args) {
        this.logger.info("レポート作成開始");
        // 現存ファイルの整理
        backupReportFile(new File(this.reportPutDirPath, FEATURE_FILE_NAME), MAX_FILE_COUNT);
        backupReportFile(new File(this.reportPutDirPath, ANDROID_FILE_NAME), MAX_FILE_COUNT);
        backupReportFile(new File(this.reportPutDirPath, ANDROID_BUNDLE_FILE_NAME), MAX_FILE_COUNT);

        // Featureレポート用ファイル分類
        File dir = new File(this.reportGetDirPath);
        String[] files = dir.list();
        TreeMap<String, ArrayList<String>> fileTree = new TreeMap<String, ArrayList<String>>();

        for (String fileName : files) {
            Matcher normalMatcher = DEFAULT_REPORT_FILE_NAME_PATTERN.matcher(fileName);
            if (normalMatcher.find()) {
                if (!fileTree.containsKey(normalMatcher.group(1))) {
                    fileTree.put(normalMatcher.group(1), new ArrayList<String>());
                }
                fileTree.get(normalMatcher.group(1)).add(fileName);
                continue;
            }
            Matcher addMatcher = ADD_REPORT_FILE_NAME_PATTERN.matcher(fileName);
            if (addMatcher.find()) {
                if (!fileTree.containsKey(addMatcher.group(1))) {
                    fileTree.put(addMatcher.group(1), new ArrayList<String>());
                }
                fileTree.get(addMatcher.group(1)).add(fileName);
                continue;
            }
            Matcher changeMatcher = CHANGE_REPORT_FILE_NAME_PATTERN.matcher(fileName);
            if (changeMatcher.find()) {
                String date = changeMatcher.group(1) + changeMatcher.group(2) + changeMatcher.group(3) + changeMatcher.group(4) + changeMatcher.group(5);
                if (!fileTree.containsKey(date)) {
                    fileTree.put(date, new ArrayList<String>());
                }
                fileTree.get(date).add(fileName);
                continue;
            }
        }

        for (ArrayList<String> fileList : fileTree.values()) {
            for (String fileName : fileList) {
                Matcher changeMatcher = CHANGE_REPORT_FILE_NAME_PATTERN.matcher(fileName);
                if (changeMatcher.find()) {
                    importFeatureReportFromExcel(fileName, this.reportGetDirPath);
                }
                else {
                    importFeatureReportFromFile(fileName, this.reportGetDirPath);
                }
            }
        }
        fileTree = null;

        System.gc();

        // Androidレポート作成
        makeFile(
            ANDROID_FILE_NAME,
            new ProcessorClamp<ReportForAndroid>() {
                public void accessProcessMethod(EntityProcessor<ReportForAndroid> processor) {
                    reportService.processAndroidReport(processor);
                }
            },
            false
        );

        // Featureレポート作成
        makeFile(
            FEATURE_FILE_NAME,
            new ProcessorClamp<Report>() {
                public void accessProcessMethod(final EntityProcessor<Report> processor) {
                    reportService.processFeatureReport(
                            new EntityProcessor<Report>() {
                                @Override
                                public void process(Report entity) {
                                    adjustUsageTypeAndReportIdInFeatureReport(entity);
                                    processor.process(entity);
                                }
                            });
                }
            },
            false
        );

        // Androidバンドルレポート作成
        makeFile(
            ANDROID_BUNDLE_FILE_NAME,
            new ProcessorClamp<ReportForAndroidBundle>() {
                public void accessProcessMethod(EntityProcessor<ReportForAndroidBundle> processor) {
                    reportService.processAndroidBundleReport(processor);
                }
            },
            true
        );

        if (this.error) {
            try {
                this.mailer.sendMail();
            }
            catch (MessagingException e) {
                this.logger.error("エラーメールを送信できませんでした。");
            }
        }
        this.logger.info("レポート作成完了");
    }

    private void importFeatureReportFromExcel(String fileName, String dirPath) {
        try {
            POIFSFileSystem file = new POIFSFileSystem(new FileInputStream(dirPath + "/" + fileName));
            HSSFWorkbook book = new HSSFWorkbook(file);
            HSSFSheet sheet = book.getSheetAt(0);
            for (int i = sheet.getFirstRowNum(); i <= sheet.getLastRowNum(); i++) {
                HSSFRow row = sheet.getRow(i);
                ArrayList<String> values = new ArrayList<String>();
                for (int j = 0; j < FORMAT.length; j++) {
                    HSSFCell cell = row.getCell(j);
                    String cellValue = cell.getStringCellValue().trim();
                    if (cellValue.equals("ﾊﾟｰﾄﾅｰSEQ")) {
                        break;
                    }
                    values.add(cellValue);
                }
                if (!values.isEmpty()) {
                    try {
                        Report entity = makeReportEntity(values.toArray(new String[0]));
                        this.reportService.addReport(entity);
                    }
                    catch (Exception e) {
                        this.error = true;
                        logger.error(fileName + "の" + i + "行目の処理中に例外発生", e);
                        this.mailer.addErrorData(fileName, i, StringUtils.join(values.toArray(), ","));
                    }
                }
            }
        }
        catch (FileNotFoundException e) {
            this.error = true;
            logger.error("Excelファイルが存在しない", e);
            this.mailer.addError("Excelファイルが存在しません。ファイル名：" + fileName);
        }
        catch (IOException e) {
            this.error = true;
            logger.error("Excelファイルの読み込みに失敗", e);
            this.mailer.addError("Excelファイルの読み込みに失敗しました。ファイル名：" + fileName);
        }
        File file = new File(dirPath + "/" + fileName);
        file.delete();
    }

    private void importFeatureReportFromFile(String fileName, String dirPath) {
        FileInputStream fis = null;
        BufferedReader readFile = null;
        try {
            try {
                fis = new FileInputStream(dirPath + "/" + fileName);
                readFile = new BufferedReader(new InputStreamReader(fis, "WINDOWS-31J"));
                String line;
                int lineCount = 1;
                while((line = readFile.readLine()) != null) {
                    line = new String(line.getBytes("UTF-8"), "UTF-8");
                    Pattern labelPattern = Pattern.compile("^ﾊﾟｰﾄﾅｰSEQ");
                    Matcher addMatcher = labelPattern.matcher(line);
                    if (addMatcher.find()) {
                        continue;
                    }
                    String[] values = line.split("\t");
                    try {
                        Report entity = makeReportEntity(values);
                        this.reportService.addReport(entity);
                    }
                    catch (Exception e) {
                        this.error = true;
                        logger.error(fileName + "の" + lineCount + "行目の処理中に例外発生", e);
                        this.mailer.addErrorData(fileName, lineCount, StringUtils.join(values, ","));
                    }
                    lineCount++;
                }
            }
            finally {
                fis.close();
                readFile.close();
            }
        }
        catch (IOException e) {
            this.error = true;
            logger.error("Excelファイルの読み込みに失敗", e);
            this.mailer.addError("ファイルの読み込みに失敗しました。ファイル名：" + fileName);
        }
        File file = new File(dirPath + "/" + fileName );
        file.delete();
    }

    // データベースに格納する値を, レポートで出力するべき値に整形する処理
    private void adjustUsageTypeAndReportIdInFeatureReport(@Nonnull Report report) {

        // Usage Type "MD" は, レポートする際には "MDN" にする
        if ("MD".equals(StringUtils.trim(report.getUsageType()))) {
            report.setUsageType("MDN");
        }

        // 一定の法則に基づいて, 価格コードを決め打ちにする.
        Pair<String, Integer> itemCategoryAndSalesPrice = Pair.of(report.getItemCategory(), report.getSalesPrice());
        if (UNIFIED_PRICE_CODE_BY_ITEM_CATEGORY_AND_SALES_PRICE.containsKey(itemCategoryAndSalesPrice)) {
            report.setPriceId(UNIFIED_PRICE_CODE_BY_ITEM_CATEGORY_AND_SALES_PRICE.get(itemCategoryAndSalesPrice));
        }
        else {
            // 知らない組合せ(=イレギュラー価格)の場合は空欄
            report.setPriceId(null);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Report makeReportEntity(String[] values) throws Exception {
        Report entity = new Report();
        Class cls = entity.getClass();

        for (int i = 0; i < FORMAT.length; i++) {
            Field field = cls.getDeclaredField(FORMAT[i]);
            Class fieldType = field.getType();
            char firstChar = FORMAT[i].charAt(0);
            char uFirstChar = Character.toUpperCase(firstChar);
            Class [] argTypes = {fieldType};
            String setterMethod = "set" + FORMAT[i].replaceFirst(String.valueOf(firstChar), String.valueOf(uFirstChar));
            Method method = cls.getMethod(setterMethod, argTypes);
            if (i < values.length && values[i].toString().trim() != null && !values[i].toString().trim().equals("")) {
                if (fieldType.equals(Date.class)) {
                    Object[] argValues = {dateFormat.parse(values[i].trim())};
                    method.invoke(entity, argValues);
                }
                else if(fieldType.equals(Integer.class)) {
                    Object[] argValues = {Integer.parseInt(values[i].trim())};
                    method.invoke(entity, argValues);
                }
                else {
                    Object[] argValues = {values[i].trim()};
                    method.invoke(entity, argValues);
                }
            }
        }
        return entity;
    }

    private <T extends Report> void makeFile(String fileName, ProcessorClamp<T> raiser, boolean isHeaderRequired) {
        try {
            final ReportFileBuilder builder = reportFileWriterFactory.create(new File(reportPutDirPath, fileName), isHeaderRequired);

            try {
                raiser.accessProcessMethod(
                    new EntityProcessor<T>() {
                        @Override
                        public void process(T entity) {
                            try {
                                builder.add(entity);
                            } catch (IOException e) {
                                // 失敗したときに処理の外まで一気に飛ばしたい(エラーメールなげたい)ので,渋々RuntimeExceptionで投げる
                                throw new OutputReportProcessFailedException(e);
                            }
                        }
                    }
                );
                builder.build();
            } finally {
                builder.close();
            }
        } catch (OutputReportProcessFailedException e) {
            this.error = true;
            this.mailer.addError("レポートファイルを出力できませんでした。ファイル名：" + fileName);
        } catch (IOException e) {
            this.error = true;
            this.mailer.addError("レポートファイルを出力できませんでした。ファイル名：" + fileName);
        }
    }

    private static interface ProcessorClamp<T> {
        void accessProcessMethod(EntityProcessor<T> processor);
    }

    private static class OutputReportProcessFailedException extends RuntimeException {
        public OutputReportProcessFailedException(IOException e) {
            super(e);
        }
    };

    private static void backupReportFile(@Nonnull File backupFile, int maxBackupCount) {
        String fileName = backupFile.getName();
        String parent = backupFile.getParent();
        File expiredBackupFile = new File(parent, fileName + "." + maxBackupCount);
        if (expiredBackupFile.exists()) {
            expiredBackupFile.delete();
        }

        for (int i = maxBackupCount - 1; i > 0; i--) {
            File backupIncrementFile = new File(parent, fileName + "." + i);
            if (backupIncrementFile.exists()) {
                backupIncrementFile.renameTo(
                    new File(parent, fileName + "." + (i + 1))
                );
            }
        }

        File newBackupFile = new File(parent, fileName);
        if (newBackupFile.exists()) {
            newBackupFile.renameTo(
                new File(parent, fileName + ".1")
            );
        }
    }
}

