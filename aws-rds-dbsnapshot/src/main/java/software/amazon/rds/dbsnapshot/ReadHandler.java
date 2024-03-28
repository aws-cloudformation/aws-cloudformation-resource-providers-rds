package software.amazon.rds.dbsnapshot;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(HandlerConfig.builder().build());
    }

    public ReadHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        return proxy.initiate("rds::describe-db-snapshot", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbSnapshotsRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBSnapshots
                ))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET))
                .done((describeRequest, describeResponse, proxyInvocation, model, context) -> {
                    final DBSnapshot dbSnapshot = describeResponse.dbSnapshots().stream().findFirst().get();
                    return ProgressEvent.success(Translator.translateToModel(dbSnapshot), context);
                });
    }
}
