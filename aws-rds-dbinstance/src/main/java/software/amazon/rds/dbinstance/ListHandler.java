package software.amazon.rds.dbinstance;

import java.util.List;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.request.ValidatedRequest;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;

public class ListHandler extends BaseHandlerStd {

    public ListHandler() {
        this(DEFAULT_DB_INSTANCE_HANDLER_CONFIG);
    }

    public ListHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final VersionedProxyClient<RdsClient> rdsProxyClient,
            final VersionedProxyClient<Ec2Client> ec2ProxyClient,
            final RequestLogger logger
    ) {
        return proxy.initiate("rds::list-db-instances", rdsProxyClient.defaultClient(), request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.describeDbInstancesRequest(request.getNextToken()))
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBInstances
                )).done((describeRequest, describeResponse, proxyInvocation, resourceModel, context) -> {
                    final List<ResourceModel> resourceModels = Translator.translateDbInstancesFromSdk(describeResponse.dbInstances());
                    return ProgressEvent.<ResourceModel, CallbackContext>builder()
                            .callbackContext(callbackContext)
                            .resourceModels(resourceModels)
                            .nextToken(describeResponse.marker())
                            .status(OperationStatus.SUCCESS)
                            .build();
                });
    }
}
