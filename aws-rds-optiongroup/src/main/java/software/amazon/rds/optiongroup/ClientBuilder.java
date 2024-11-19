package software.amazon.rds.optiongroup;

import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.rds.common.client.BaseSdkClientProvider;
import software.amazon.rds.common.client.RdsUserAgentProvider;

public class ClientBuilder extends BaseSdkClientProvider<RdsClientBuilder, RdsClient> {

    private static final int MAX_ATTEMPTS = 6;

    private RdsClientBuilder setUserAgentAndRetryPolicy(final RdsClientBuilder builder) {
        return builder.overrideConfiguration(cfg -> {
            cfg.putAdvancedOption(USER_AGENT_PREFIX, RdsUserAgentProvider.getUserAgentPrefix())
                    .putAdvancedOption(USER_AGENT_SUFFIX, RdsUserAgentProvider.getUserAgentSuffix())
                    .retryStrategy(b -> b.maxAttempts(MAX_ATTEMPTS));
        });
    }

    @Override
    public RdsClient getClient() {
        return setHttpClient(setUserAgentAndRetryPolicy(RdsClient.builder())).build();
    }
}
