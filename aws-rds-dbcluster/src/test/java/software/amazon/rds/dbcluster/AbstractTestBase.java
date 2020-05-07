package software.amazon.rds.dbcluster;

import com.google.common.collect.Lists;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.ScalingConfigurationInfo;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;

import org.slf4j.LoggerFactory;
import software.amazon.cloudformation.proxy.ProxyClient;

public class AbstractTestBase {

    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;


    protected static final Integer BACKUP_RETENTION_PERIOD;
    protected static final Integer BACKTRACK_WINDOW;
    protected static final String DBCLUSTER_IDENTIFIER;
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

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final ResourceModel RESOURCE_MODEL_ON_RESTORE;
    protected static final ResourceModel RESOURCE_MODEL_ON_RESTORE_IN_TIME;

    protected static final DBCluster DBCLUSTER_ACTIVE;
    protected static final DBCluster DBCLUSTER_ACTIVE_NO_ROLE;
    protected static final DBCluster DBCLUSTER_DELETED;
    protected static final DBCluster DBCLUSTER_INPROGRESS;

    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss:SSS Z");
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

        delegate = LoggerFactory.getLogger("testing");
        logger = new LoggerProxy();

        BACKUP_RETENTION_PERIOD = 1;
        BACKTRACK_WINDOW = 1;
        DBCLUSTER_IDENTIFIER = "my-sample-dbcluster";
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
                .build();

        RESOURCE_MODEL_ON_RESTORE_IN_TIME = ResourceModel.builder()
                .dBClusterIdentifier(null)
                .dBClusterParameterGroupName(null)
                .sourceDBClusterIdentifier(SOURCE_IDENTIFIER)
                .engineMode(null)
                .masterUsername(USER_NAME)
                .masterUserPassword(USER_PASSWORD)
                .build();

        DBCLUSTER_ACTIVE = DBCluster.builder()
                .dbClusterArn("arn")
                .associatedRoles(software.amazon.awssdk.services.rds.model.DBClusterRole.builder().roleArn(ROLE_ARN).featureName(ROLE_FEATURE).build())
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
                .build();

    }

    static ProxyClient<RdsClient> MOCK_PROXY(
        final AmazonWebServicesClientProxy proxy,
        final RdsClient rdsClient
    ) {
        return new ProxyClient<RdsClient>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            ResponseT
            injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            CompletableFuture<ResponseT>
            injectCredentialsAndInvokeV2Async(RequestT request,
                Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
            IterableT
            injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
                return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
            }

            @Override
            public RdsClient client() {
                return rdsClient;
            }
        };
    }
}
