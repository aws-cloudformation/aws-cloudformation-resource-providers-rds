package software.amazon.rds.dbinstance;

import lombok.NonNull;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.LambdaWrapper;

public class RdsClientBuilder {
    public static RdsClient getClient() {
        return RdsClient.builder()
                .httpClient(LambdaWrapper.HTTP_CLIENT)
                .build();
    }

    public static RdsClient getClient(@NonNull final String apiVersion) {
        return RdsClient.builder()
                .httpClient(LambdaWrapper.HTTP_CLIENT)
                .overrideConfiguration(cfg -> cfg.addExecutionInterceptor(new ExecutionInterceptor() {
                    @Override
                    public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest ctx, ExecutionAttributes attrs) {
                        return ctx.httpRequest()
                                .toBuilder()
                                .putRawQueryParameter("Version", apiVersion)
                                .build();
                    }
                }))
                .build();
    }
}
