package software.amazon.rds.dbcluster;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterRole;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.VpcSecurityGroupMembership;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.Logger;

import java.util.function.Function;
import java.util.stream.Collectors;

import static software.amazon.rds.dbcluster.Translator.listTagsForResourceRequest;

public class ReadHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient,
                                                                          final Logger logger) {
        return proxy.initiate("rds::describe-db-cluster", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDbClustersRequest)
                .makeServiceCall((describeDbClustersRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(describeDbClustersRequest, proxyInvocation.client()::describeDBClusters))
                .done((describeDbClustersRequest, describeDbClustersResponse, proxyInvocation, model, context) -> {

                    final Function<DBClusterRole, software.amazon.rds.dbcluster.DBClusterRole> roleTransform = (DBClusterRole dbClusterRole) -> new software.amazon.rds.dbcluster.DBClusterRole(dbClusterRole.roleArn(), dbClusterRole.featureName());
                    final DBCluster targetDBCluster = describeDbClustersResponse.dbClusters().stream().findFirst().get();
                    final ListTagsForResourceResponse listTagsForResourceResponse = proxyInvocation.injectCredentialsAndInvokeV2(listTagsForResourceRequest(targetDBCluster.dbClusterArn()), proxyInvocation.client()::listTagsForResource);

                    return ProgressEvent.defaultSuccessHandler(ResourceModel.builder()
                            // read only properties GetAtt
                            .endpoint(Endpoint.builder()
                                    .address(targetDBCluster.endpoint())
                                    .port(targetDBCluster.port().toString()).build())
                            .readEndpoint(ReadEndpoint.builder()
                                    .address(targetDBCluster.readerEndpoint()).build())

                            .associatedRoles(targetDBCluster.associatedRoles().stream().map(roleTransform).collect(Collectors.toList()))
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
                            .build());
                });
    }
}
