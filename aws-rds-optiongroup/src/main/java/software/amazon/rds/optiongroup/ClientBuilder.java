package software.amazon.rds.optiongroup;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {

    private final static int MAX_RETRIES = 5;

    private ClientBuilder() {
    }

    private static final RetryPolicy RETRY_POLICY = RetryPolicy.builder()
            .numRetries(MAX_RETRIES)
            .retryCondition(RetryCondition.defaultRetryCondition())
            .build();

    public static RdsClient getClient() {
        return RdsClient.builder()
                .httpClient(LambdaWrapper.HTTP_CLIENT)
                .overrideConfiguration(
                        ClientOverrideConfiguration.builder()
                                .retryPolicy(RETRY_POLICY)
                                .build()
                )
                .build();
    }
}
