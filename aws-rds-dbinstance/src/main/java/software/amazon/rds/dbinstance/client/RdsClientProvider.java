package software.amazon.rds.dbinstance.client;

import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Supplier;

import lombok.NonNull;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.rds.common.client.BaseSdkClientProvider;
import software.amazon.rds.common.client.RdsUserAgentProvider;

public class RdsClientProvider extends BaseSdkClientProvider<RdsClientBuilder, RdsClient> {

    public static final String VERSION_QUERY_PARAM = "Version";

    public RdsClientProvider() {
        super();
    }

    public RdsClientProvider(final Supplier<SdkHttpClient> httpClientSupplier) {
        super(httpClientSupplier);
    }

    // The reason this method embeds an already existing logic from {@code setUserAgent} is because
    // every invocation of {@code builder.overrideConfiguration} overrides previous configuration modifications.
    // Hence, we need to pack all modifications in a single {@code builder.overrideConfiguration} call.
    private RdsClientBuilder setUserAgentAndApiVersion(final RdsClientBuilder builder, final String apiVersion) {
        return builder.overrideConfiguration(cfg -> cfg
                .putAdvancedOption(USER_AGENT_PREFIX, RdsUserAgentProvider.getUserAgentPrefix())
                .putAdvancedOption(USER_AGENT_SUFFIX, RdsUserAgentProvider.getUserAgentSuffix())
                .addExecutionInterceptor(new ExecutionInterceptor() {
                    @Override
                    public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest ctx, ExecutionAttributes attrs) {
                        return ctx.httpRequest()
                                .toBuilder()
                                .putRawQueryParameter(VERSION_QUERY_PARAM, apiVersion)
                                .build();
                    }
                }));
    }

    @Override
    public RdsClient getClient() {
        return setUserAgent(setHttpClient(RdsClient.builder())).build();
    }

    public RdsClient getClientForApiVersion(@NonNull final String apiVersion) {
        return setUserAgentAndApiVersion(setHttpClient(RdsClient.builder()), apiVersion).build();
    }
}
