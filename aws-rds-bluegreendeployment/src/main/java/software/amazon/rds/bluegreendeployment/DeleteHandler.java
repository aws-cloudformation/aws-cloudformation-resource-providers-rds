package software.amazon.rds.bluegreendeployment;

import org.apache.commons.lang3.BooleanUtils;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;

public class DeleteHandler extends BaseHandlerStd {

    public DeleteHandler() {
        this(HandlerConfig.builder().build());
    }

    public DeleteHandler(final HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> client,
            final RequestLogger logger
    ) {
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> Commons.execOnce(progress, () -> {
                    final BlueGreenDeployment blueGreenDeployment = fetchBlueGreenDeployment(client, progress.getResourceModel());
                    final String sourceDBInstanceIdentifier = Translator.getDBInstanceIdentifier(blueGreenDeployment.source());
                    progress.getCallbackContext().setObservedStatus(blueGreenDeployment.status());
                    progress.getCallbackContext().setSourceDBInstanceIdentifier(sourceDBInstanceIdentifier);
                    return progress;
                }, CallbackContext::isStatusClarified, CallbackContext::setStatusClarified))
                .then(progress -> deleteBlueGreenDeployment(proxy, client, progress))
                .then(progress -> {
                    // Only delete the source instance if the switchover is completed
                    if (BooleanUtils.isTrue(progress.getResourceModel().getDeleteSource()) &&
                            BlueGreenDeploymentStatus.SwitchoverCompleted.equalsString(progress.getCallbackContext().getObservedStatus())) {
                        return deleteSourceDBInstance(proxy, client, progress, progress.getCallbackContext().getSourceDBInstanceIdentifier());
                    }
                    return progress;
                })
                .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteSourceDBInstance(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress,
            final String sourceInstanceIdentifier
    ) {
        return proxy.initiate("rds::delete-source-db-instance", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.deleteDbInstanceRequest(sourceInstanceIdentifier))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((request, client) -> client.injectCredentialsAndInvokeV2(
                        request,
                        client.client()::deleteDBInstance
                ))
                .stabilize((request, response, client, model, context) -> isDBInstanceDeleted(client, sourceInstanceIdentifier))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_BLUE_GREEN_DEPLOYMENT_ERROR_RULE_SET
                ))
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteBlueGreenDeployment(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<RdsClient> proxyClient,
            final ProgressEvent<ResourceModel, CallbackContext> progress
    ) {
        // Target can not be deleted if the switchover is completed
        final boolean deleteTarget = BooleanUtils.isTrue(progress.getResourceModel().getDeleteTarget()) &&
                !BlueGreenDeploymentStatus.SwitchoverCompleted.equalsString(progress.getCallbackContext().getObservedStatus());

        return proxy.initiate("rds::delete-blue-green-deployment", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(model -> Translator.deleteBlueGreenDeploymentRequest(model, deleteTarget))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((request, client) -> client.injectCredentialsAndInvokeV2(
                        request,
                        client.client()::deleteBlueGreenDeployment
                ))
                .stabilize((request, response, client, model, context) -> isBlueGreenDeploymentDeleted(client, model))
                .handleError((request, exception, client, model, context) -> Commons.handleException(
                        ProgressEvent.progress(model, context),
                        exception,
                        DEFAULT_BLUE_GREEN_DEPLOYMENT_ERROR_RULE_SET
                ))
                .progress();
    }
}
