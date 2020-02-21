package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;

import java.util.stream.Collectors;

public class ReadHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {


        ResourceModel model = request.getDesiredResourceState();
        return proxy.initiate("rds::describe-db-cluster", proxyClient, model, callbackContext)
                .request(Translator::describeDbClustersRequest)
                .call((r, c) -> c.injectCredentialsAndInvokeV2(r, c.client()::describeDBClusters))
                .done((r) -> {

                    final DBCluster targetDBCluster = r.dbClusters().stream().findFirst().get();
                    final ListTagsForResourceResponse listTagsForResourceResponse = proxyClient.injectCredentialsAndInvokeV2(Translator.listTagsForResourceRequest(targetDBCluster.dbClusterArn()), proxyClient.client()::listTagsForResource);

                    return ProgressEvent.defaultSuccessHandler(ResourceModel.builder()
                            .applyImmediately(model.getApplyImmediately())
                            .associatedRoles(targetDBCluster.associatedRoles().stream().map(
                                    dbClusterRole -> DBClusterRole.builder().roleArn(dbClusterRole.roleArn())
                                    .featureName(dbClusterRole.featureName()).build()
                            ).collect(Collectors.toList()))
                            .availabilityZones(targetDBCluster.availabilityZones())
                            .backtrackWindow(Translator.castToInt(targetDBCluster.backtrackWindow()))
                            .backupRetentionPeriod(targetDBCluster.backupRetentionPeriod())
                            .databaseName(targetDBCluster.databaseName())
                            .dBClusterIdentifier(targetDBCluster.dbClusterIdentifier())
                            .dBClusterParameterGroupName(targetDBCluster.dbClusterParameterGroup())
                            .dBSubnetGroupName(targetDBCluster.dbSubnetGroup())
                            .deletionProtection(targetDBCluster.deletionProtection())
                            .enableCloudwatchLogsExports(targetDBCluster.enabledCloudwatchLogsExports())
                            .enableHttpEndpoint(targetDBCluster.httpEndpointEnabled())
                            .enableIAMDatabaseAuthentication(targetDBCluster.iamDatabaseAuthenticationEnabled())
                            .engine(targetDBCluster.engine())
                            .engineMode(targetDBCluster.engineMode())
                            .engineVersion(targetDBCluster.engineVersion())
                            .kmsKeyId(targetDBCluster.kmsKeyId())
                            .masterUsername(targetDBCluster.masterUsername())
                            .port(targetDBCluster.port())
                            .preferredBackupWindow(targetDBCluster.preferredBackupWindow())
                            .preferredMaintenanceWindow(targetDBCluster.preferredMaintenanceWindow())
                            .replicationSourceIdentifier(targetDBCluster.replicationSourceIdentifier())
                            .scalingConfiguration(Translator.translateScalingConfigurationFromSdk(targetDBCluster.scalingConfigurationInfo()))
                            .storageEncrypted(targetDBCluster.storageEncrypted())
                            .tags(Translator.translateTagsFromSdk(listTagsForResourceResponse.tagList()))
                            .vpcSecurityGroupIds(targetDBCluster.vpcSecurityGroups().stream().map(VpcSecurityGroupMembership::vpcSecurityGroupId).collect(Collectors.toList()))
                            /*....*/
                            .build());
                });
    }
}
