package software.amazon.rds.dbclustersnapshot;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {
  public static RdsClient getClient() {
    return RdsClient.builder()
            .httpClient(LambdaWrapper.HTTP_CLIENT)
            .build();
  }
}
