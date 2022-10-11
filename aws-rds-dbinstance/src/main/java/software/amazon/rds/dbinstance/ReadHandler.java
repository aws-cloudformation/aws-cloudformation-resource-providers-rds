package software.amazon.rds.dbinstance;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.common.config.RuntimeConfig;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;
import software.amazon.rds.dbinstance.request.ValidatedRequest;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(RuntimeConfig.loadFrom(resource(RuntimeConfig.RUNTIME_PROPERTIES)));
    }

    public ReadHandler(final RuntimeConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ValidatedRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final VersionedProxyClient<RdsClient> rdsProxyClient,
            final VersionedProxyClient<Ec2Client> ec2ProxyClient,
            final Logger logger
    ) {
        return proxy.initiate("rds::describe-db-instance", rdsProxyClient.defaultClient(), request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbInstancesRequest)
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBInstances
                ))
                .handleError((describeRequest, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_DB_INSTANCE_ERROR_RULE_SET
                ))
                .done((describeRequest, describeResponse, proxyInvocation, resourceModel, context) -> {
                    final DBInstance dbInstance = describeResponse.dbInstances().get(0);
                    return ProgressEvent.success(Translator.translateDbInstanceFromSdk(dbInstance), context);
                });
    }
}
