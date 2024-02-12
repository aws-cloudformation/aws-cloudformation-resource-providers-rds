package software.amazon.rds.customdbengineversion;


import java.util.Collection;
import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBEngineVersion;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;


public class ReadHandler extends BaseHandlerStd {
    public ReadHandler() {
        this(CUSTOM_ENGINE_VERSION_HANDLER_CONFIG_10H);
    }

    public ReadHandler(HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient
    ) {
        return proxy.initiate("rds::read-custom-db-engine-version", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbEngineVersionsRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeDbEngineVersionsRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeDbEngineVersionsRequest, proxyInvocation.client()::describeDBEngineVersions))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_CUSTOM_DB_ENGINE_VERSION_ERROR_RULE_SET,
                        requestLogger))
                .done((describeCustomDbEngineVersionRequest, describeCustomDbEngineVersionResponse, proxyInvocation, model, context) -> {
                    final List<DBEngineVersion> engineVersions = describeCustomDbEngineVersionResponse.dbEngineVersions();

                    if (engineVersions.isEmpty()) {
                        return ProgressEvent.failed(model, context, HandlerErrorCode.NotFound,
                                "CustomDBEngineVersion " + model.getEngineVersion() + " not found");
                    }

                    return ProgressEvent.success(Translator.translateFromSdk(engineVersions.get(0)), context);
                });
    }

}
