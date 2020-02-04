package jp.co.dwango.ddex.report;
​
import java.util.Map;
​
import jp.co.dwango.ddex.core.di.CoreModule;
​
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
​
public class ReportModule extends CoreModule {
​
    @Override
    public void configure() {
        super.configure();
​
        install(
                new FactoryModuleBuilder().implement(ReportFileBuilder.class, ReportFileBuilderImpl.class).build(ReportFileBuilderFactoryImpl.class)
                );
​
        // メールアドレス設定
        Map<String, String> mailAddresses = (Map<String, String>)this.config.get("mailAddress");
        if (mailAddresses == null) {
            throw new RuntimeException("ddex.yml上にメールアドレス(mailAddress要素)の設定が無いため, アラートメールの送信不可");
        }
​
        for (Map.Entry<String, String> entry : mailAddresses.entrySet()) {
            bind(String.class)
                .annotatedWith(Names.named("mailAddress/" + entry.getKey()))
                .toInstance(entry.getValue());
        }
    }
​
}
