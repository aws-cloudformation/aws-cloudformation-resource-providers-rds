package software.amazon.rds.dbinstance;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.LambdaWrapper;

public class RdsClientBuilder {
    public static RdsClient getClient() {
        return RdsClient.builder()
                .httpClient(LambdaWrapper.HTTP_CLIENT)
                .build();
    }
}
