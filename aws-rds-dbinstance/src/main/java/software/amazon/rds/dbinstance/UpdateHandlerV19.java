package software.amazon.rds.dbinstance;

import org.apache.commons.lang3.BooleanUtils;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class UpdateHandlerV19 extends BaseUpdateHandler {

    public UpdateHandlerV19(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> updateDbInstance(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final ProxyClient<RdsClient> rdsProxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        return proxy.initiate("rds::modify-db-instance-v19", rdsProxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(resourceModel -> TranslatorV19.modifyDbInstanceRequest(
                        request.getPreviousResourceState(),
                        request.getDesiredResourceState(),
                        BooleanUtils.isTrue(request.getRollback()))
                )
                .backoffDelay(config.getBackoff())
                .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        modifyRequest,
                        proxyInvocation.client()::modifyDBInstance
                ))
                .stabilize((modifyRequest, response, proxyInvocation, model, context) -> withProbing(
                        context,
                        "update-db-instance-available",
                        3,
                        () -> isDbInstanceStabilized(proxyInvocation, model)
                ))
                .handleError((modifyRequest, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        MODIFY_DB_INSTANCE_ERROR_RULE_SET
                ))
                .progress();
    }
}
