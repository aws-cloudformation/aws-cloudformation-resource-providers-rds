package software.amazon.rds.dbcluster;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;

import java.util.function.Function;

import static software.amazon.rds.dbcluster.ModelAdapter.setDefaults;
import static software.amazon.rds.dbcluster.Translator.cloudwatchLogsExportConfiguration;
import static software.amazon.rds.dbcluster.Translator.removeRoleFromDbClusterRequest;

public class UpdateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<RdsClient> proxyClient,
        final Logger logger) {

      return ProgressEvent.progress(setDefaults(request.getDesiredResourceState()), callbackContext)
          .then(progress -> modifyDBCluster(proxy, proxyClient, progress, cloudwatchLogsExportConfiguration(request)))
          .then(progress -> waitForDBClusterAvailableStatus(proxy, proxyClient, progress))
          .then(progress -> removeAssociatedRoles(proxy, proxyClient, progress, setDefaults(request.getPreviousResourceState()).getAssociatedRoles()))
          .then(progress -> addAssociatedRoles(proxy, proxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
          .then(progress -> tagResource(proxy, proxyClient, progress))
          .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> removeAssociatedRoles(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<RdsClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        final List<DBClusterRole> roles) {
    final ResourceModel model = progress.getResourceModel();
    final CallbackContext callbackContext = progress.getCallbackContext();
    for(final DBClusterRole dbClusterRole: Optional.ofNullable(roles).orElse(Collections.emptyList())) {
      final ProgressEvent<ResourceModel, CallbackContext> progressEvent =  proxy.initiate("rds::remove-roles-to-dbcluster", proxyClient, model, callbackContext)
          .translateToServiceRequest(modelRequest -> removeRoleFromDbClusterRequest(modelRequest.getDBClusterIdentifier(), dbClusterRole.getRoleArn(), dbClusterRole.getFeatureName()))
          .makeServiceCall((modelRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(modelRequest, proxyInvocation.client()::removeRoleFromDBCluster))
          .stabilize((removeRoleFromDbClusterRequest, removeRoleFromDBClusterResponse, proxyInvocation, modelRequest, callbackContext1) ->
              isRoleStabilized(proxyInvocation, modelRequest, dbClusterRole, false))
          .success();
      if (!progressEvent.isSuccess()) return progressEvent;
    }
    return ProgressEvent.progress(model, callbackContext);
  }
}
