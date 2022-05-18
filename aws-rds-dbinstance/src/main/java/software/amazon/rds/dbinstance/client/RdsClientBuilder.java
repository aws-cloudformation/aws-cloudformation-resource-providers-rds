package software.amazon.rds.dbinstance.client;

import java.util.function.Supplier;

import lombok.NonNull;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.LambdaWrapper;

public class RdsClientBuilder {

    public static final String VERSION_QUERY_PARAM = "Version";

    public static final Supplier<SdkHttpClient> LAMBDA_HTTP_CLIENT_SUPPLIER = () -> LambdaWrapper.HTTP_CLIENT;

    public final Supplier<SdkHttpClient> httpClientSupplier;

    public RdsClientBuilder() {
        this(LAMBDA_HTTP_CLIENT_SUPPLIER);
    }

    public RdsClientBuilder(final Supplier<SdkHttpClient> httpClientSupplier) {
        super();
        this.httpClientSupplier = httpClientSupplier;
    }

    public RdsClient getClient() {
        return RdsClient.builder()
                .httpClient(httpClientSupplier.get())
                .build();
    }

    public RdsClient getClient(@NonNull final String apiVersion) {
        return RdsClient.builder()
                .httpClient(httpClientSupplier.get())
                .overrideConfiguration(cfg -> cfg.addExecutionInterceptor(new ExecutionInterceptor() {
                    @Override
                    public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest ctx, ExecutionAttributes attrs) {
                        return ctx.httpRequest()
                                .toBuilder()
                                .putRawQueryParameter(VERSION_QUERY_PARAM, apiVersion)
                                .build();
                    }
                }))
                .build();
    }
}
