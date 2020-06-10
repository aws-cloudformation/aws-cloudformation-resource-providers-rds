package software.amazon.rds.dbclusterparametergroup;

import com.amazonaws.AmazonServiceException;
import java.time.Duration;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.exceptions.TerminalException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.delay.Constant;

public class UpdateHandler extends BaseHandlerStd {
    private static final String AVAILABLE = "available";
    protected static final Constant BACKOFF_STRATEGY = Constant.of().timeout(Duration.ofMinutes(120L)).delay(Duration.ofSeconds(30L)).build();

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        final ResourceModel model = request.getDesiredResourceState();
        final boolean parametersUpdated = !model.getParameters().equals(request.getPreviousResourceState().getParameters());
        return ProgressEvent.progress(model, callbackContext)
            .then(progress -> {
                if (!parametersUpdated) return progress; // if same params then skip update
                return proxy.initiate("rds::update-db-cluster-parameter-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(Translator::resetDbClusterParameterGroupRequest)
                    .backoffDelay(BACKOFF_STRATEGY)
                    .makeServiceCall((resetGroupRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(resetGroupRequest, proxyInvocation.client()::resetDBClusterParameterGroup))
                    .done((resetGroupRequest, resetGroupResponse, proxyInvocation, resourceModel, context) -> applyParameters(proxy, proxyInvocation, resourceModel, context));
            })
            .then(progress -> {
                if (!parametersUpdated) return progress; // if same params then skip stabilization
                final ResourceModel resourceModel = progress.getResourceModel();
                final CallbackContext cxt = progress.getCallbackContext();

                if (!cxt.isClusterStabilized()) { // if not stabilized then we keep describing clusters and memoizing into the set

                    final DescribeDbClustersResponse describeDbClustersResponse = proxyClient.injectCredentialsAndInvokeV2(Translator.describeDbClustersRequest(cxt.getMarker()), proxyClient.client()::describeDBClusters);

                    if(describeDbClustersResponse.dbClusters().stream()
                        .filter(dbCluster -> dbCluster.dbClusterParameterGroup().equals(resourceModel.getDBClusterParameterGroupName())) // all db clusters that use param group
                        .allMatch(dbCluster -> dbCluster.status().equals(AVAILABLE))) { // if all stabilized then move to the next page

                        if (describeDbClustersResponse.marker() != null) { // more pages left
                            cxt.setMarker(describeDbClustersResponse.marker());
                            progress.setCallbackDelaySeconds(30); // if there are more to describe
                        } else { // nothing left to stabilized
                            cxt.setClusterStabilized(true);
                        }
                    } else {
                        progress.setCallbackDelaySeconds(30); // if some still in transition status need some delay to describe
                    }
                }
                progress.setCallbackContext(cxt);
                return progress;
            })
            .then(progress ->
                describeDbClusterParameterGroup(proxy, proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .done((paramGroupRequest, paramGroupResponse, rdsProxyClient, resourceModel, cxt) -> tagResource(paramGroupResponse, proxyClient, resourceModel, cxt, request.getDesiredResourceTags())))
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }
}
