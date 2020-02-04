package jp.co.dwango.ddex.report;
​
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
​
import javax.annotation.Nonnull;
import javax.inject.Named;
​
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
​
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
​
import jp.co.dwango.ddex.core.util.TextConverter;
import jp.co.dwango.ddex.db.report.entity.Report;
​
class ReportFileBuilderImpl implements ReportFileBuilder {
    private final File outputPath;
    private final File workPath;
    private final OutputStream outputStream;
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
​
    private static final Charset OUTPUT_CHARSET = Charset.forName("Windows-31j");
    private static final String[] REPLACE_BEFORE = {
        "､", "｡", "&", "$", "\"", ":", ";", "?", "=", "<", ">", "\\", "'", ",", "\n"
    };
    private static final String[] REPLACE_AFTER = {
        "、", "。", "＆", "＄", "”", "：", "；", "？", "＝", "＜", "＞", "￥", "’", "，", " "
    };
    private static final byte[] LINEFEED = "\n".getBytes(OUTPUT_CHARSET);
​
    private static final Joiner TSV_JOINER = Joiner.on('\t').useForNull("");
​
    private static final Map<String, String> COLUMN_NAME_MAP = new ImmutableMap.Builder<String, String>()
        .put("partnerSeq", "ﾊﾟｰﾄﾅｰSEQ")
        .put("partnerName", "ﾊﾟｰﾄﾅｰ名称")
        .put("beginDate", "配信開始日")
        .put("itemCategory", "ITEM CATEGORY")
        .put("productType", "PRODUCT TYPE")
        .put("grId", "規格番号")
        .put("icpn", "ﾓﾊﾞｲﾙUPC")
        .put("isrc", "ﾓﾊﾞｲﾙISRC")
        .put("usageType", "USAGE TYPE")
        .put("salesPrice", "希望小売価格（税抜）")
        .put("priceId", "PRICE CODE")
        .put("title", "商品ﾀｲﾄﾙ（日本語）")
        .put("titleYomi", "商品ﾀｲﾄﾙ（カナ）")
        .put("titleEng", "商品ﾀｲﾄﾙ（英語）")
        .put("artist", "ｱｰﾃｨｽﾄ名（日本語）")
        .put("artistYomi", "ｱｰﾃｨｽﾄ名（カナ）")
        .put("artistEng", "ｱｰﾃｨｽﾄ名（英語）")
        .put("musicIsrc", "ISRC")
        .put("musicTitle", "楽曲タイトル（日本語）")
        .put("packageCode", "ﾊﾟｯｹｰｼﾞ品番")
        .put("packageUpc", "ﾊﾟｯｹｰｼﾞUPC")
        .put("packageTitle", "ﾊﾟｯｹｰｼﾞﾀｲﾄﾙ（日本語）")
        .put("packageTitleEng", "ﾊﾟｯｹｰｼﾞﾀｲﾄﾙ（英語）")
        .put("packageArtist", "ﾊﾟｯｹｰｼﾞｱｰﾃｨｽﾄ（日本語）")
        .put("packageArtistEng", "ﾊﾟｯｹｰｼﾞｱｰﾃｨｽﾄ（英語）")
        .put("copyrightId", "著作権管理団体ID")
        .put("copyrightCode", "著作権管理団体商品ｺｰﾄﾞ")
        .put("copyrightTitle", "著作権管理団体原題（日本語）")
        .put("copyrightSongwriter", "著作権管理団体作詞者名")
        .put("copyrightTranslater", "著作権管理団体訳詩者名")
        .put("copyrightComposer", "著作権管理団体作曲者名")
        .put("copyrightArranger", "著作権管理団体編曲者名")
        .put("copyrightArtist", "著作権管理団体ｱｰﾃｨｽﾄ名")
        .put("artistCode", "ｱｰﾃｨｽﾄｺｰﾄﾞ")
        .put("priceTaxIn", "税込み小売価格")
        .put("priceTaxOut", "税抜き小売価格")
        .put("releaseDate", "ﾊﾟｯｹｰｼﾞ発売日")
        .build();
​
    @Inject
    public ReportFileBuilderImpl(@Named("tmpDirPath") String tmpDirPath, @Assisted File outputPath, @Assisted boolean isHeaderRequired) throws IOException, FileNotFoundException {
        this.outputPath = outputPath;
​
        // 処理が完全に終わる(=build()が呼ばれる)まで,outputPathへの配置を遅延する
        // ※用途上あまり意味は無いが,デグレードしないようにそうしている
        workPath = File.createTempFile("report", ".tsv", new File(tmpDirPath));
        outputStream = new BufferedOutputStream(
            new FileOutputStream(workPath)
        );
​
        // 必要なときだけヘッダ出力
        if (isHeaderRequired) {
            List<String> headers = Lists.newArrayListWithExpectedSize(ReportBatchImpl.FORMAT.length);
            for (String column : ReportBatchImpl.FORMAT) {
                headers.add(COLUMN_NAME_MAP.get(column));
            }
​
            outputStream.write(
                TextConverter.convertToNormalizedBytes(TSV_JOINER.join(headers), OUTPUT_CHARSET)
            );
            outputStream.write(LINEFEED);
        }
    }
​
    @Override
    public ReportFileBuilder add(@Nonnull Report report) throws IOException {
        Map<String, Object> reportMap = report.makeMap();
        List<Object> lineArray = new ArrayList<Object>();
        for (String column : ReportBatchImpl.FORMAT) {
            Object value = reportMap.get(column);
            if (value instanceof Date) {
                value = dateFormat.format(value);
            }
            if (value instanceof String && ((String)value).contains("\t")) {
                // データ内にタブ区切り文字があるとtsvが崩れてしまうため半角スペースへ置換する
                lineArray.add(((String)value).replace("\t", " "));
            } else {
                lineArray.add(value);
            }
        }
​
        String output = StringUtils.replaceEach(
            TSV_JOINER.join(lineArray),
            REPLACE_BEFORE,
            REPLACE_AFTER
        );
​
        outputStream.write(TextConverter.convertToNormalizedBytes(output, OUTPUT_CHARSET));
        outputStream.write(LINEFEED);
​
        return this;
    }
​
    @Override
    public void build() throws IOException {
        close();
        FileUtils.copyFile(workPath, outputPath);
    }
​
    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}

