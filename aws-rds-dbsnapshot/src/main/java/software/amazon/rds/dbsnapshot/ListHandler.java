package software.amazon.rds.dbsnapshot;

import java.util.List;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

public class ListHandler extends BaseHandlerStd {

    public ListHandler() {
        this(HandlerConfig.builder().build());
    }

    public ListHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        return proxy.initiate("rds::list-db-snapshots", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.describeDbSnapshotsRequest(request.getNextToken()))
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBSnapshots
                )).done((describeRequest, describeResponse, proxyInvocation, resourceModel, context) -> {
                    final List<ResourceModel> resourceModels = describeResponse.dbSnapshots()
                            .stream().map(Translator::translateToModel).collect(Collectors.toList());
                    return ProgressEvent.<ResourceModel, CallbackContext>builder()
                            .callbackContext(callbackContext)
                            .resourceModels(resourceModels)
                            .nextToken(describeResponse.marker())
                            .status(OperationStatus.SUCCESS)
                            .build();
                });
    }
}
