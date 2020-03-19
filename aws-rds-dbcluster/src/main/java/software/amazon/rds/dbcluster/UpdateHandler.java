package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;

import java.util.function.Function;

import static software.amazon.rds.dbcluster.ModelAdapter.setDefaults;
import static software.amazon.rds.dbcluster.Translator.cloudwatchLogsExportConfiguration;

public class UpdateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return proxy.initiate("rds::update-dbcluster", proxyClient, setDefaults(request.getDesiredResourceState()), callbackContext)
                // returns the input as is
                .request(Function.identity())
                .retry(CONSTANT)
                // this skips the call and goes directly to stabilization
                .call(EMPTY_CALL).progress()
                .then(progress -> {
                    // checks if db cluster has been modified
                    if (progress.getCallbackContext().isModified()) return progress;
                    return modifyDBCluster(proxy, proxyClient, progress, cloudwatchLogsExportConfiguration(request));
                })
                .then(progress -> waitForDBCluster(proxy, proxyClient, progress, DBClusterStatus.Available))
                .then(progress -> removeAssociatedRoles(proxy, proxyClient, progress, setDefaults(request.getPreviousResourceState()).getAssociatedRoles()))
                .then(progress -> addAssociatedRoles(proxy, proxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
                .then(progress -> tagResource(proxy, proxyClient, progress))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
