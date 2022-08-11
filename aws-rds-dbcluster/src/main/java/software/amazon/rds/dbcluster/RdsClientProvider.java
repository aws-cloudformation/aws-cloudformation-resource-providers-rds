package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.rds.common.client.BaseSdkClientProvider;
import software.amazon.rds.common.test.ExcludeFromJacocoGeneratedReport;

public class RdsClientProvider extends BaseSdkClientProvider<RdsClientBuilder, RdsClient> {

    @ExcludeFromJacocoGeneratedReport
    @Override
    public RdsClient getClient() {
        return setHttpClient(setUserAgent(RdsClient.builder())).build();
    }
}
