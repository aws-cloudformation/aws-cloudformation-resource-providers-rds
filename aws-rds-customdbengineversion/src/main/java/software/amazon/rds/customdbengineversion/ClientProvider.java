package software.amazon.rds.customdbengineversion;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.rds.common.client.BaseSdkClientProvider;

public class ClientProvider extends BaseSdkClientProvider<RdsClientBuilder, RdsClient> {

    @Override
    public RdsClient getClient() {
        return setHttpClient(setUserAgent(RdsClient.builder())).build();
    }
}
