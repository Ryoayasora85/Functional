-ckage jp.co.dwango.ddex.report;
​
import java.util.ArrayList;
import javax.mail.MessagingException;
​
import org.apache.commons.lang3.StringUtils;
​
import jp.co.dwango.ddex.core.resource.mail.Mail;
import lombok.Delegate;
import lombok.Value;
​
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
​
public class ReportMail implements Mail {
​
    @Value(staticConstructor="of")
    private static class SourceOfError {
        String fileName;
        int line;
        String data;
    }
​
    private ArrayList<String> errorMessages = Lists.newArrayList();
    private ArrayList<SourceOfError> sourceOfErrors = Lists.newArrayList();
​
    private interface Reset {
        void reset();
    };
​
    @Delegate(excludes=Reset.class)
    private final Mail mail;
​
    private final String mailAdressToDeveloper;
    private final String mailAdressToCdCheck;
​
    @Inject
    public ReportMail(Mail mail, @Named("mailAddress/developer") String mailAdressToDeveloper, @Named("mailAddress/cdCheck") String mailAdressToCdCheck) {
        this.mail = mail;
        this.mailAdressToDeveloper = mailAdressToDeveloper;
        this.mailAdressToCdCheck = mailAdressToCdCheck;
        setSender("ddex_report@dwango.co.jp");
    }
​
    @Override
    public void reset() {
        mail.reset();
        setSender("ddex_report@dwango.co.jp");
        this.errorMessages = Lists.newArrayList();
        this.sourceOfErrors = Lists.newArrayList();
    }
​
    public void sendMail() throws MessagingException {
        addRecipient(mailAdressToDeveloper);
        addRecipient(mailAdressToCdCheck);
        String subject = "【レポート】レポート作成エラー";
        StringBuilder bodyBuilder = new StringBuilder("以下のエラーが発生しました。\n\n");
        if (this.errorMessages.size() > 0) {
            bodyBuilder.append("■").append(StringUtils.join(this.errorMessages, "\n■")).append("\n");
        }
        if (this.sourceOfErrors.size() > 0) {
            bodyBuilder.append("■以下のデータが不正です。\n");
            for (SourceOfError sourceOfError : this.sourceOfErrors) {
                bodyBuilder
                    .append("ファイル：").append(sourceOfError.getFileName())
                    .append("\t行：").append(sourceOfError.getLine())
                    .append("\tデータ：").append(sourceOfError.getData())
                    .append("\n");
            }
        }
        setSubject(subject);
        setText(bodyBuilder.toString());
        send();
        reset();
    }
​
    public void addError(String errorMessage) {
        this.errorMessages.add(errorMessage);
    }
​
    public void addErrorData(String fileName, int line, String data) {
        this.sourceOfErrors.add(SourceOfError.of(fileName, line, data));
    }
}

