package software.amazon.rds.dbinstance;


import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(HandlerConfig.builder().probingEnabled(true).build());
    }

    public ReadHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger
    ) {
        return proxy.initiate("rds::describe-db-instance", rdsProxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbInstancesRequest)
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBInstances
                ))
                .handleError((describeRequest, exception, client, model, context) -> handleException(
                        ProgressEvent.progress(model, context),
                        exception
                ))
                .done((describeRequest, describeResponse, proxyInvocation, resourceModel, context) -> {
                    final DBInstance dbInstance = describeResponse.dbInstances().stream().findFirst().get();
                    return ProgressEvent.success(Translator.translateDbInstanceFromSdk(dbInstance), context);
                });
    }
}
