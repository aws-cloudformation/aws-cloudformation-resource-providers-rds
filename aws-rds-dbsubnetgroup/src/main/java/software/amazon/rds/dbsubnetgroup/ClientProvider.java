package software.amazon.rds.dbsubnetgroup;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.cloudformation.LambdaWrapper;
import software.amazon.rds.common.client.BaseSdkClientProvider;

public class ClientProvider extends BaseSdkClientProvider<RdsClientBuilder, RdsClient> {

    @Override
    public RdsClient getClient() {
        return setEndpointOverride(setHttpClient(setUserAgent(RdsClient.builder()))).build();
    }
}
