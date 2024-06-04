package software.amazon.rds.globalcluster;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.GlobalClusterMember;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.awssdk.services.rds.model.GlobalCluster;


public class ReadHandler extends BaseHandlerStd {
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return proxy.initiate("rds::read-global-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeGlobalClustersRequest)
                .makeServiceCall((describeGlobalClustersRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeGlobalClustersRequest, proxyInvocation.client()::describeGlobalClusters))
                .done((describeGlobalClustersRequest, describeGlobalClustersResponse, proxyInvocation, model, context) -> {

                    final GlobalCluster targetGlobalCluster = describeGlobalClustersResponse.globalClusters().stream().findFirst().get();

                    return ProgressEvent.defaultSuccessHandler(toResourceModel(targetGlobalCluster));
                });
    }

    @VisibleForTesting
    ResourceModel toResourceModel(GlobalCluster cluster) {

        ResourceModel.ResourceModelBuilder builder = ResourceModel.builder();

        builder.globalClusterIdentifier(cluster.globalClusterIdentifier());
        builder.engine(cluster.engine());
        builder.engineVersion(cluster.engineVersion());
        builder.storageEncrypted(cluster.storageEncrypted());
        builder.deletionProtection(cluster.deletionProtection());
        builder.engineLifecycleSupport(cluster.engineLifecycleSupport());

        if (cluster.hasGlobalClusterMembers()) {

            List<GlobalClusterMember> globalClusterMembers = cluster.globalClusterMembers();

            for (GlobalClusterMember globalClusterMember : globalClusterMembers) {
                if (globalClusterMember.isWriter()) {
                    builder.sourceDBClusterIdentifier(globalClusterMember.dbClusterArn());

                    break;
                }
            }
        }

        return builder.build();
    }
}
