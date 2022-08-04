package software.amazon.rds.optiongroup;

import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.rds.common.client.BaseSdkClientProvider;
import software.amazon.rds.common.client.RdsUserAgentProvider;

public class ClientBuilder extends BaseSdkClientProvider<RdsClientBuilder, RdsClient> {

    private final static int MAX_RETRIES = 5;

    private static final RetryPolicy RETRY_POLICY = RetryPolicy.builder()
            .numRetries(MAX_RETRIES)
            .retryCondition(RetryCondition.defaultRetryCondition())
            .build();

    private RdsClientBuilder setUserAgentAndRetryPolicy(final RdsClientBuilder builder) {
        return builder.overrideConfiguration(cfg -> {
            cfg.putAdvancedOption(USER_AGENT_PREFIX, RdsUserAgentProvider.getUserAgentPrefix())
                    .putAdvancedOption(USER_AGENT_SUFFIX, RdsUserAgentProvider.getUserAgentSuffix())
                    .retryPolicy(RETRY_POLICY);
        });
    }

    @Override
    public RdsClient getClient() {
        return setHttpClient(setUserAgentAndRetryPolicy(RdsClient.builder())).build();
    }
}
