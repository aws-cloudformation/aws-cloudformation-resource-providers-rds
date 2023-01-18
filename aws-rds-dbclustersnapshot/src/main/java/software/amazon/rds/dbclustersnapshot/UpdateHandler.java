package software.amazon.rds.dbclustersnapshot;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

public class UpdateHandler extends BaseHandlerStd { // FIXME: "rds:DescribeDBClusters" to be added to FAS
    public UpdateHandler() { this(HandlerConfig.builder().build()); }

    public UpdateHandler(final HandlerConfig config) { super(config); }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {
        return ProgressEvent.failed(
                request.getDesiredResourceState(),
                callbackContext,
                HandlerErrorCode.NotUpdatable,
                "Resource is immutable"
        );
    }
}
