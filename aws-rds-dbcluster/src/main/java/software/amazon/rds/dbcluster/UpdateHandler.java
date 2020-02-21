package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;
import java.util.function.Function;
import software.amazon.rds.dbcluster.DBClusterStatus.Status;

import static software.amazon.rds.dbcluster.ModelAdapter.setDefaults;

public class UpdateHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return proxy.initiate("rds::update-dbcluster", proxyClient, setDefaults(request.getDesiredResourceState()), callbackContext)
                .request(Function.identity())
                .retry(EXPONENTIAL)
                .call((m, c) -> m).progress()
                .then(progress -> modifyDBCluster(proxy, proxyClient, progress, Translator.cloudwatchLogsExportConfiguration(request)))
                .then(progress -> waitForDBCluster(proxy, proxyClient, progress, Status.Available))
                .then(progress -> removeAssociatedRoles(proxy, proxyClient, progress, setDefaults(request.getPreviousResourceState()).getAssociatedRoles()))
                .then(progress -> addAssociatedRoles(proxy, proxyClient, progress, progress.getResourceModel().getAssociatedRoles()))
                .then(progress -> tagResource(proxy, proxyClient, progress))
                .then(progress -> ProgressEvent.defaultSuccessHandler(progress.getResourceModel()));
    }
}
