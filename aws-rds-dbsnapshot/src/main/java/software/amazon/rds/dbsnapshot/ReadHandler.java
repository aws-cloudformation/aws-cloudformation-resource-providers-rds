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
        this(DEFAULT_HANDLER_CONFIG);
    }

    public ReadHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> client,
            final Logger logger
    ) {
        return proxy.initiate("rds::describe-db-snapshot", client, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDBSnapshotRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBSnapshots
                ))
                .handleError((describeRequest, exception, rdsClient, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_SNAPSHOT_ERROR_RULE_SET
                ))
                .done((describeRequest, describeResponse, proxyInvocation, model, context) -> {
                    final DBSnapshot dbSnapshot = describeResponse.dbSnapshots().get(0);
                    return ProgressEvent.success(Translator.translateDBSnapshotFromSdk(dbSnapshot), context);
                });
    }
}
