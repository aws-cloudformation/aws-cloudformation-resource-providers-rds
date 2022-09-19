package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.rds.common.annotations.ExcludeFromJacocoGeneratedReport;
import software.amazon.rds.common.client.BaseSdkClientProvider;

public class RdsClientProvider extends BaseSdkClientProvider<RdsClientBuilder, RdsClient> {

    @ExcludeFromJacocoGeneratedReport
    @Override
    public RdsClient getClient() {
        return setHttpClient(setUserAgent(RdsClient.builder())).build();
    }
}
