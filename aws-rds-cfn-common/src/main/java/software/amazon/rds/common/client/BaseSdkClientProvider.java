package software.amazon.rds.common.client;

import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

import java.util.function.Supplier;

import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.cloudformation.LambdaWrapper;

public abstract class BaseSdkClientProvider<B extends AwsClientBuilder<B, C> & AwsSyncClientBuilder<B, C>, C extends SdkClient> {

    public static final Supplier<SdkHttpClient> LAMBDA_HTTP_CLIENT_SUPPLIER = () -> LambdaWrapper.HTTP_CLIENT;

    protected final Supplier<SdkHttpClient> httpClientSupplier;

    protected BaseSdkClientProvider() {
        this(LAMBDA_HTTP_CLIENT_SUPPLIER);
    }

    public BaseSdkClientProvider(final Supplier<SdkHttpClient> httpClientSupplier) {
        super();
        this.httpClientSupplier = httpClientSupplier;
    }

    protected B setHttpClient(final B builder) {
        return builder.httpClient(httpClientSupplier.get());
    }

    protected B setUserAgent(final B builder) {
        return builder.overrideConfiguration(cfg -> {
            cfg.putAdvancedOption(USER_AGENT_PREFIX, RdsUserAgentProvider.getUserAgentPrefix())
                    .putAdvancedOption(USER_AGENT_SUFFIX, RdsUserAgentProvider.getUserAgentSuffix());
        });
    }

    protected B setEndpointOverride(final B builder) {
        if (!RdsEndpointOverrideProvider.getEndpointOverride().isPresent()) {
            return builder;
        }
        return builder.endpointOverride(RdsEndpointOverrideProvider.getEndpointOverride().get());
    }

    public abstract C getClient();
}
