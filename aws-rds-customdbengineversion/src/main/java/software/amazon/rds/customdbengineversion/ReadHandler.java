package software.amazon.rds.customdbengineversion;


import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBEngineVersion;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;


public class ReadHandler extends BaseHandlerStd {
    public ReadHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public ReadHandler(HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        return proxy.initiate("rds::read-custom-db-engine-version", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbEngineVersionsRequest)
                .makeServiceCall((describeDbEngineVersionsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeDbEngineVersionsRequest, proxyInvocation.client()::describeDBEngineVersions))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET))
                .done((describeCustomDbEngineVersionRequest, describeCustomDbEngineVersionResponse, proxyInvocation, model, context) -> {
                    final DBEngineVersion engineVersion = describeCustomDbEngineVersionResponse.dbEngineVersions().stream().findFirst().get();
                    return ProgressEvent.success(Translator.translateToModel(engineVersion), context);
                });
    }

}
