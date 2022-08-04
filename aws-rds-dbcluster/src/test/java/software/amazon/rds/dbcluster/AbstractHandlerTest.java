package software.amazon.rds.dbcluster;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.GlobalCluster;
import software.amazon.awssdk.services.rds.model.ScalingConfigurationInfo;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.test.AbstractTestBase;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBCluster, ResourceModel, CallbackContext> {

    protected static final String LOGICAL_RESOURCE_IDENTIFIER = "dbcluster";

    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;


    protected static final Integer BACKUP_RETENTION_PERIOD;
    protected static final Integer BACKTRACK_WINDOW;
    protected static final String DBCLUSTER_IDENTIFIER;
    protected static final String DBGLOBALCLUSTER_IDENTIFIER;
    protected static final String DBCLUSTER_PARAMETER_GROUP_NAME;
    protected static final String SNAPSHOT_IDENTIFIER;
    protected static final String SOURCE_IDENTIFIER;
    protected static final String ENGINE;
    protected static final String ENGINE_MODE;
    protected static final Integer PORT;
    protected static final String USER_NAME;
    protected static final String USER_PASSWORD;
    protected static final String ROLE_ARN;
    protected static final String ROLE_FEATURE;
    protected static final DBClusterRole ROLE;
    protected static final DBClusterRole ROLE_WITH_EMPTY_FEATURE;

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final ResourceModel RESOURCE_MODEL_EMPTY_VPC;
    protected static final ResourceModel RESOURCE_MODEL_ON_RESTORE;
    protected static final ResourceModel RESOURCE_MODEL_ON_RESTORE_IN_TIME;
    protected static final ResourceModel RESOURCE_MODEL_WITH_GLOBAL_CLUSTER;
    protected static final DBCluster DBCLUSTER_ACTIVE;
    protected static final DBCluster DBCLUSTER_ACTIVE_NO_ROLE;
    protected static final DBCluster DBCLUSTER_DELETED;
    protected static final DBCluster DBCLUSTER_INPROGRESS;
    protected static final DBCluster DBCLUSTER_ACTIVE_DELETION_ENABLED;
    protected static final DBClusterSnapshot DBCLUSTER_SNAPSHOT;
    protected static final DBClusterSnapshot DBCLUSTER_SNAPSHOT_AVAILABLE;
    protected static final DBClusterSnapshot DBCLUSTER_SNAPSHOT_CREATING;
    protected static final GlobalCluster GLOBAL_CLUSTER;
    protected static final DBClusterSnapshot DBCLUSTER_SNAPSHOT_FAILED;
    protected static final List<String> VPC_SG_IDS;


    protected static final Set<Tag> TAG_LIST;
    protected static final Set<Tag> TAG_LIST_EMPTY;
    protected static final Set<Tag> TAG_LIST_ALTER;
    protected static final Tagging.TagSet TAG_SET;

    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss:SSS Z");
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

        delegate = LoggerFactory.getLogger("testing");
        logger = new LoggerProxy();

        BACKUP_RETENTION_PERIOD = 1;
        BACKTRACK_WINDOW = 1;
        DBCLUSTER_IDENTIFIER = "my-sample-dbcluster";
        DBGLOBALCLUSTER_IDENTIFIER = "my-sample-global-cluster";
        DBCLUSTER_PARAMETER_GROUP_NAME = "default.aurora5.6";
        SNAPSHOT_IDENTIFIER = "my-sample-dbcluster-snapshot";
        SOURCE_IDENTIFIER = "my-source-dbcluster-identifier";
        ENGINE = "aurora";
        ENGINE_MODE = "serverless";
        PORT = 3306;
        USER_NAME = "username";
        USER_PASSWORD = "xxx";

        ROLE_ARN = "sampleArn";
        ROLE_FEATURE = "sampleFeature";
        ROLE = DBClusterRole.builder().roleArn(ROLE_ARN).featureName(ROLE_FEATURE).build();
        ROLE_WITH_EMPTY_FEATURE = DBClusterRole.builder().roleArn(ROLE_ARN).build();
        VPC_SG_IDS = Arrays.asList("vpc-sg-id-1", "vpc-sg-id-2");

        RESOURCE_MODEL = ResourceModel.builder()
                .associatedRoles(Lists.newArrayList(ROLE))
                .backtrackWindow(BACKTRACK_WINDOW)
                .dBClusterIdentifier(DBCLUSTER_IDENTIFIER)
                .dBClusterParameterGroupName(DBCLUSTER_PARAMETER_GROUP_NAME)
                .engine(ENGINE)
                .backupRetentionPeriod(BACKUP_RETENTION_PERIOD)
                .port(PORT)
                .masterUsername(USER_NAME)
                .masterUserPassword(USER_PASSWORD)
                .vpcSecurityGroupIds(VPC_SG_IDS)
                .build();

        RESOURCE_MODEL_EMPTY_VPC = ResourceModel.builder()
                .associatedRoles(Lists.newArrayList(ROLE))
                .backtrackWindow(BACKTRACK_WINDOW)
                .dBClusterIdentifier(DBCLUSTER_IDENTIFIER)
                .dBClusterParameterGroupName(DBCLUSTER_PARAMETER_GROUP_NAME)
                .engine(ENGINE)
                .backupRetentionPeriod(BACKUP_RETENTION_PERIOD)
                .port(PORT)
                .masterUsername(USER_NAME)
                .masterUserPassword(USER_PASSWORD)
                .build();

        RESOURCE_MODEL_ON_RESTORE = ResourceModel.builder()
                .dBClusterIdentifier(DBCLUSTER_IDENTIFIER)
                .dBClusterParameterGroupName(DBCLUSTER_PARAMETER_GROUP_NAME)
                .snapshotIdentifier(SNAPSHOT_IDENTIFIER)
                .engineMode(ENGINE_MODE)
                .masterUsername(USER_NAME)
                .masterUserPassword(USER_PASSWORD)
                .scalingConfiguration(
                        ScalingConfiguration.builder()
                                .autoPause(true)
                                .minCapacity(1)
                                .maxCapacity(10)
                                .secondsUntilAutoPause(5)
                                .build()
                )
                .vpcSecurityGroupIds(VPC_SG_IDS)
                .build();

        RESOURCE_MODEL_ON_RESTORE_IN_TIME = ResourceModel.builder()
                .dBClusterIdentifier(null)
                .dBClusterParameterGroupName(null)
                .sourceDBClusterIdentifier(SOURCE_IDENTIFIER)
                .engineMode(null)
                .masterUsername(USER_NAME)
                .masterUserPassword(USER_PASSWORD)
                .vpcSecurityGroupIds(VPC_SG_IDS)
                .build();

        RESOURCE_MODEL_WITH_GLOBAL_CLUSTER = ResourceModel.builder()
                .associatedRoles(Lists.newArrayList(ROLE))
                .backtrackWindow(BACKTRACK_WINDOW)
                .dBClusterIdentifier(DBCLUSTER_IDENTIFIER)
                .dBClusterParameterGroupName(DBCLUSTER_PARAMETER_GROUP_NAME)
                .engine(ENGINE)
                .backupRetentionPeriod(BACKUP_RETENTION_PERIOD)
                .port(PORT)
                .masterUsername(USER_NAME)
                .masterUserPassword(USER_PASSWORD)
                .globalClusterIdentifier(DBGLOBALCLUSTER_IDENTIFIER)
                .vpcSecurityGroupIds(VPC_SG_IDS)
                .build();

        DBCLUSTER_ACTIVE = DBCluster.builder()
                .dbClusterArn("arn")
                .associatedRoles(
                        software.amazon.awssdk.services.rds.model.DBClusterRole.builder().roleArn(ROLE_ARN).featureName(ROLE_FEATURE).build())
                .dbClusterIdentifier(RESOURCE_MODEL.getDBClusterIdentifier())
                .deletionProtection(false)
                .engine(RESOURCE_MODEL.getEngine())
                .port(RESOURCE_MODEL.getPort())
                .masterUsername(RESOURCE_MODEL.getMasterUsername())
                .status(DBClusterStatus.Available.toString())
                .scalingConfigurationInfo(
                        ScalingConfigurationInfo.builder()
                                .autoPause(true)
                                .maxCapacity(10)
                                .minCapacity(1)
                                .secondsUntilAutoPause(5)
                                .build()
                )
                .build();

        DBCLUSTER_ACTIVE_DELETION_ENABLED = DBCluster.builder()
                .dbClusterArn("arn")
                .associatedRoles(
                        software.amazon.awssdk.services.rds.model.DBClusterRole.builder().roleArn(ROLE_ARN).featureName(ROLE_FEATURE).build())
                .dbClusterIdentifier(RESOURCE_MODEL.getDBClusterIdentifier())
                .deletionProtection(true)
                .engine(RESOURCE_MODEL.getEngine())
                .port(RESOURCE_MODEL.getPort())
                .masterUsername(RESOURCE_MODEL.getMasterUsername())
                .status(DBClusterStatus.Available.toString())
                .scalingConfigurationInfo(
                        ScalingConfigurationInfo.builder()
                                .autoPause(true)
                                .maxCapacity(10)
                                .minCapacity(1)
                                .secondsUntilAutoPause(5)
                                .build()
                )
                .build();

        DBCLUSTER_SNAPSHOT = DBClusterSnapshot.builder()
                .build();

        DBCLUSTER_SNAPSHOT_AVAILABLE = DBClusterSnapshot.builder()
                .status("available")
                .build();

        DBCLUSTER_SNAPSHOT_CREATING = DBClusterSnapshot.builder()
                .status("creating")
                .build();

        DBCLUSTER_SNAPSHOT_FAILED = DBClusterSnapshot.builder()
                .status("failed")
                .build();

        GLOBAL_CLUSTER = GlobalCluster.builder()
                .build();


        DBCLUSTER_ACTIVE_NO_ROLE = DBCluster.builder()
                .dbClusterIdentifier(RESOURCE_MODEL.getDBClusterIdentifier())
                .engine(RESOURCE_MODEL.getEngine())
                .port(RESOURCE_MODEL.getPort())
                .masterUsername(RESOURCE_MODEL.getMasterUsername())
                .status(DBClusterStatus.Available.toString())
                .build();

        DBCLUSTER_DELETED = DBCluster.builder()
                .dbClusterIdentifier(RESOURCE_MODEL.getDBClusterIdentifier())
                .engine(RESOURCE_MODEL.getEngine())
                .port(RESOURCE_MODEL.getPort())
                .masterUsername(RESOURCE_MODEL.getMasterUsername())
                .status(DBClusterStatus.Deleted.toString())
                .build();

        DBCLUSTER_INPROGRESS = DBCluster.builder()
                .dbClusterIdentifier(RESOURCE_MODEL.getDBClusterIdentifier())
                .engine(RESOURCE_MODEL.getEngine())
                .port(RESOURCE_MODEL.getPort())
                .masterUsername(RESOURCE_MODEL.getMasterUsername())
                .status(DBClusterStatus.Creating.toString())
                .deletionProtection(false)
                .build();

        TAG_LIST_EMPTY = ImmutableSet.of();

        TAG_LIST = ImmutableSet.of(
                Tag.builder().key("foo").value("bar").build()
        );

        TAG_LIST_ALTER = ImmutableSet.of(
                Tag.builder().key("bar").value("baz").build(),
                Tag.builder().key("fizz").value("buzz").build()
        );

        TAG_SET = Tagging.TagSet.builder()
                .systemTags(ImmutableSet.of(
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("system-tag-1").value("system-tag-value1").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("system-tag-2").value("system-tag-value2").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("system-tag-3").value("system-tag-value3").build()
                )).stackTags(ImmutableSet.of(
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("stack-tag-1").value("stack-tag-value1").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("stack-tag-2").value("stack-tag-value2").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("stack-tag-3").value("stack-tag-value3").build()
                )).resourceTags(ImmutableSet.of(
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("resource-tag-1").value("resource-tag-value1").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("resource-tag-2").value("resource-tag-value2").build(),
                        software.amazon.awssdk.services.rds.model.Tag.builder().key("resource-tag-3").value("resource-tag-value3").build()
                )).build();
    }

    static <ClientT> ProxyClient<ClientT> MOCK_PROXY(final AmazonWebServicesClientProxy proxy, final ClientT client) {
        return new BaseProxyClient<>(proxy, client);
    }

    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    protected abstract ProxyClient<Ec2Client> getEc2Proxy();

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context
    ) {
        return getHandler().handleRequest(getProxy(), request, context, getRdsProxy(), getEc2Proxy(), logger);
    }

    @Override
    protected String getLogicalResourceIdentifier() {
        return LOGICAL_RESOURCE_IDENTIFIER;
    }

    @Override
    protected void expectResourceSupply(final Supplier<DBCluster> supplier) {
        when(getRdsProxy()
                .client()
                .describeDBClusters(any(DescribeDbClustersRequest.class))
        ).then(res -> DescribeDbClustersResponse.builder()
                .dbClusters(supplier.get())
                .build()
        );
    }
}
