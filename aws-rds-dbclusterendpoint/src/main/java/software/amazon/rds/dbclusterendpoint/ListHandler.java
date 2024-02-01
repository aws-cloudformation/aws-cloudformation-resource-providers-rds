package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

import java.util.List;

public class ListHandler extends BaseHandlerStd {


    public ListHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public ListHandler(HandlerConfig config) {
        super(config);
    }


    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext) {

        return proxy.initiate("rds::list-db-cluster-endpoints", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.describeDbClustersEndpointRequest(request.getNextToken()))
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBClusterEndpoints
                )).done((describeRequest, describeResponse, proxyInvocation, resourceModel, context) -> {
                    final List<ResourceModel> resourceModels = Translator.translateDbClusterEndpointFromSdk(describeResponse.dbClusterEndpoints()
                            // Only Custom endpoints have primary identifier, therefore we need to filter them in our list handler
                            .stream().filter(endpoint -> CUSTOM_ENDPOINT.equals(endpoint.endpointType())));
                    return ProgressEvent.<ResourceModel, CallbackContext>builder()
                            .callbackContext(callbackContext)
                            .resourceModels(resourceModels)
                            .nextToken(describeResponse.marker())
                            .status(OperationStatus.SUCCESS)
                            .build();
                });
    }
}
