package software.amazon.rds.dbinstance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.Endpoint;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.test.AbstractTestBase;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBInstance, ResourceModel, CallbackContext> {

    protected static final String LOGICAL_RESOURCE_IDENTIFIER = "dbinstance";

    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;

    protected static final List<Tag> TAG_LIST_EMPTY;
    protected static final List<Tag> TAG_LIST;
    protected static final List<Tag> TAG_LIST_ALTER;

    protected static final Integer ALLOCATED_STORAGE = 10;
    protected static final Integer ALLOCATED_STORAGE_INCR = 20;
    protected static final Integer ALLOCATED_STORAGE_DECR = 5;
    protected static final String ASSOCIATED_ROLE_NAME = "db-instance-role-name";
    protected static final String ASSOCIATED_ROLE_ARN = "db-instance-role-arn";
    protected static final List<DBInstanceRole> ASSOCIATED_ROLES;
    protected static final List<DBInstanceRole> ASSOCIATED_ROLES_ALTER;
    protected static final Boolean AUTO_MINOR_VERSION_UPGRADE_YES = true;
    protected static final Boolean AUTO_MINOR_VERSION_UPGRADE_NO = false;
    protected static final String AVAILABILITY_ZONE = "db-instance-availability-zone";
    protected static final Integer BACKUP_RETENTION_PERIOD_DEFAULT = 1;
    protected static final String CA_CERTIFICATE_IDENTIFIER_EMPTY = null;
    protected static final String CA_CERTIFICATE_IDENTIFIER_NON_EMPTY = "db-instance-ca-cert-identifier";
    protected static final String CHARACTER_SET_NAME = "db-instance-character-set-name";
    protected static final Boolean COPY_TAGS_TO_SNAPSHOT_YES = true;
    protected static final Boolean COPY_TAGS_TO_SNAPSHOT_NO = false;
    protected static final String DB_CLUSTER_IDENTIFIER_EMPTY = null;
    protected static final String DB_CLUSTER_IDENTIFIER_NON_EMPTY = "db-instance-db-cluster-identifier";
    protected static final String DB_INSTANCE_IDENTIFIER_EMPTY = null;
    protected static final String DB_INSTANCE_IDENTIFIER_NON_EMPTY = "db-instance-identifier";
    protected static final String DB_INSTANCE_ARN_EMPTY = null;
    protected static final String DB_INSTANCE_ARN_NON_EMPTY = "db-instance-arn";
    protected static final String DB_INSTANCE_CLASS_DEFAULT = "db.m5.large";
    protected static final String DB_INSTANCE_STATUS_AVAILABLE = "available";
    protected static final String DB_INSTANCE_STATUS_CREATING = "creating";
    protected static final String DB_INSTANCE_STATUS_DELETING = "deleting";
    protected static final String DB_INSTANCE_STATUS_MODIFYING = "modifying";
    protected static final String DB_INSTANCE_STATUS_FAILED = "failed";
    protected static final String DB_NAME = "db-instance-db-name";
    protected static final String DB_PARAMETER_GROUP_NAME_DEFAULT = "default";
    protected static final String DB_PARAMETER_GROUP_NAME_ALTER = "alternative-parameter-group";
    protected static final String DB_SECURITY_GROUP_DEFAULT = "default";
    protected static final String DB_SECURITY_GROUP_ID = "db-security-group-id";
    protected static final String DB_SECURITY_GROUP_VPC_ID = "db-security-group-vpc-id";
    protected static final String DB_SNAPSHOT_IDENTIFIER_EMPTY = null;
    protected static final String DB_SNAPSHOT_IDENTIFIER_NON_EMPTY = "db-snapshot-identifier";
    protected static final String DB_SUBNET_GROUP_NAME_DEFAULT = "default";
    protected static final Boolean DELETE_AUTOMATED_BACKUPS_YES = true;
    protected static final Boolean DELETE_AUTOMATED_BACKUPS_NO = false;
    protected static final Boolean DELETION_PROTECTION_YES = true;
    protected static final Boolean DELETION_PROTECTION_NO = false;
    protected static final String DOMAIN_EMPTY = null;
    protected static final String DOMAIN_IAM_ROLE_NAME_EMPTY = null;
    protected static final Boolean ENABLE_IAM_DATABASE_AUTHENTICATION_YES = true;
    protected static final Boolean ENABLE_IAM_DATABASE_AUTHENTICATION_NO = false;
    protected static final Boolean ENABLE_PERFORMANCE_INSIGHTS_YES = true;
    protected static final Boolean ENABLE_PERFORMANCE_INSIGHTS_NO = false;
    protected static final String ENGINE_AURORA = "aurora";
    protected static final String ENGINE_AURORA_MYSQL = "aurora-mysql";
    protected static final String ENGINE_AURORA_POSTGRESQL = "aurora-postgresql";
    protected static final String ENGINE_MARIADB = "mariadb";
    protected static final String ENGINE_MYSQL = "mysql";
    protected static final String ENGINE_ORACLE_EE = "oracle-ee";
    protected static final String ENGINE_ORACLE_SE2 = "oracle-se2";
    protected static final String ENGINE_ORACLE_SE1 = "oracle-se1";
    protected static final String ENGINE_ORACLE_SE = "oracle-se";
    protected static final String ENGINE_POSTGRES = "postgres";
    protected static final String ENGINE_SQLSERVER_EE = "sqlserver-ee";
    protected static final String ENGINE_SQLSERVER_SE = "sqlserver-se";
    protected static final String ENGINE_SQLSERVER_EX = "sqlserver-ex";
    protected static final String ENGINE_SQLSERVER_WEB = "sqlserver-web";
    protected static final String ENGINE_VERSION_MYSQL_56 = "5.6";
    protected static final String ENGINE_VERSION_MYSQL_80 = "8.0";
    protected static final Integer IOPS_DEFAULT = 10_000;
    protected static final Integer IOPS_INCR = 20_000;
    protected static final Integer IOPS_DECR = 5_000;
    protected static final String KMS_KEY_ID_EMPTY = null;
    protected static final String LICENSE_MODEL_LICENSE_INCLUDED = "license-included";
    protected static final String LICENSE_MODEL_BRING_YOUR_OWN_LICENSE = "bring-your-own-license";
    protected static final String LICENSE_MODEL_GENERAL_PUBLIC_LICENSE = "general-public-license";
    protected static final String MASTER_USERNAME = "master-username";
    protected static final String MASTER_USER_PASSWORD = "xxx";
    protected static final Integer MAX_ALLOCATED_STORAGE_DEFAULT = 1000;
    protected static final Integer MONITORING_INTERVAL_DEFAULT = 0;
    protected static final String MONITORING_ROLE_ARN = "monitoring-role-arn";
    protected static final Boolean MULTI_AZ_YES = true;
    protected static final Boolean MULTI_AZ_NO = false;
    protected static final String OPTION_GROUP_NAME_MYSQL_DEFAULT = "default:mysql-5-6";
    protected static final String PERFORMANCE_INSIGHTS_KMS_ID_EMPTY = null;
    protected static final Integer PERFORMANCE_INSIGHTS_RETENTION_PERIOD_DEFAULT = 7;
    protected static final Integer PORT_DEFAULT = 3306;
    protected static final String PREFERRED_BACKUP_WINDOW_EMPTY = null;
    protected static final String PREFERRED_BACKUP_WINDOW_NON_EMPTY = "03:00–11:00";
    protected static final String PREFERRED_MAINTENANCE_WINDOW_NON_EMPTY = "03:00–11:00";
    protected static final String PREFERRED_MAINTENANCE_WINDOW_EMPTY = null;
    protected static final String PROCESSOR_FEATURE_NAME = "processor-feature-name";
    protected static final String PROCESSOR_FEATURE_VALUE = "processor-feature-value";
    protected static final ProcessorFeature PROCESSOR_FEATURE;
    protected static final Integer PROMOTION_TIER_DEFAULT = 1;
    protected static final Boolean PUBLICLY_ACCESSIBLE_YES = true;
    protected static final Boolean PUBLICLY_ACCESSIBLE_NO = false;
    protected static final String SOURCE_DB_INSTANCE_IDENTIFIER_EMPTY = null;
    protected static final String SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY = "source-db-instance-identifier";
    protected static final String SOURCE_REGION_EMPTY = null;
    protected static final Boolean STORAGE_ENCRYPTED_YES = true;
    protected static final Boolean STORAGE_ENCRYPTED_NO = false;
    protected static final String STORAGE_TYPE_STANDARD = "standard";
    protected static final String STORAGE_TYPE_GP2 = "gp2";
    protected static final String STORAGE_TYPE_IO1 = "io1";
    protected static final String TIMEZONE_DEFAULT = null;
    protected static final Boolean USE_DEFAULT_PROCESSOR_FEATURES_YES = true;
    protected static final Boolean USE_DEFAULT_PROCESSOR_FEATURES_NO = false;
    protected static final String VPC_SECURITY_GROUP_NAME_DEFAULT = "default";

    protected static final String MSG_ALREADY_EXISTS_ERR = "DBInstance already exists";
    protected static final String MSG_NOT_FOUND_ERR = "DBInstance not found";
    protected static final String MSG_RUNTIME_ERR = "Runtime error";

    protected static final ResourceModel RESOURCE_MODEL_NO_IDENTIFIER;
    protected static final ResourceModel RESOURCE_MODEL_ALTER;
    protected static final ResourceModel RESOURCE_MODEL_READ_REPLICA;
    protected static final ResourceModel RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT;

    protected static final DBInstance DB_INSTANCE_BASE;
    protected static final DBInstance DB_INSTANCE_ACTIVE;
    protected static final DBInstance DB_INSTANCE_DELETING;
    protected static final DBInstance DB_INSTANCE_MODIFYING;
    protected static final DBInstance DB_INSTANCE_EMPTY_PORT;

    protected static Constant TEST_BACKOFF_DELAY = Constant.of()
            .delay(Duration.ofSeconds(1L))
            .timeout(Duration.ofSeconds(10L))
            .build();

    static {
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
        delegate = LoggerFactory.getLogger("testing");
        logger = new LoggerProxy();

        TAG_LIST_EMPTY = ImmutableList.of();
        TAG_LIST = ImmutableList.of(
                Tag.builder().key("foo").value("bar").build()
        );

        TAG_LIST_ALTER = ImmutableList.of(
                Tag.builder().key("bar").value("baz").build(),
                Tag.builder().key("fizz").value("buzz").build()
        );

        ASSOCIATED_ROLES = ImmutableList.of(
                DBInstanceRole.builder()
                        .featureName(ASSOCIATED_ROLE_NAME)
                        .roleArn(ASSOCIATED_ROLE_ARN)
                        .build()
        );
        ASSOCIATED_ROLES_ALTER = ImmutableList.of(
                DBInstanceRole.builder()
                        .featureName(ASSOCIATED_ROLE_NAME + "-foo")
                        .roleArn(ASSOCIATED_ROLE_ARN + "-foo")
                        .build(),
                DBInstanceRole.builder()
                        .featureName(ASSOCIATED_ROLE_NAME + "-bar")
                        .roleArn(ASSOCIATED_ROLE_ARN + "-bar")
                        .build()
        );

        PROCESSOR_FEATURE = ProcessorFeature.builder()
                .name(PROCESSOR_FEATURE_NAME)
                .value(PROCESSOR_FEATURE_VALUE)
                .build();


        RESOURCE_MODEL_NO_IDENTIFIER = ResourceModel.builder()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_EMPTY)
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER_EMPTY)
                .dBName(DB_NAME)
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                .engine(ENGINE_MYSQL)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .iops(IOPS_DEFAULT)
                .masterUsername(MASTER_USERNAME)
                .masterUserPassword(MASTER_USER_PASSWORD)
                .port(PORT_DEFAULT)
                .build();

        RESOURCE_MODEL_ALTER = ResourceModel.builder()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .associatedRoles(ASSOCIATED_ROLES_ALTER)
                .autoMinorVersionUpgrade(AUTO_MINOR_VERSION_UPGRADE_YES)
                .availabilityZone(AVAILABILITY_ZONE)
                .backupRetentionPeriod(BACKUP_RETENTION_PERIOD_DEFAULT)
                .cACertificateIdentifier(CA_CERTIFICATE_IDENTIFIER_NON_EMPTY)
                .characterSetName(CHARACTER_SET_NAME)
                .copyTagsToSnapshot(COPY_TAGS_TO_SNAPSHOT_YES)
                .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_EMPTY)
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER_NON_EMPTY)
                .dBName(DB_NAME)
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                .dBSnapshotIdentifier(DB_SNAPSHOT_IDENTIFIER_EMPTY)
                .dBSubnetGroupName(DB_SUBNET_GROUP_NAME_DEFAULT)
                .deleteAutomatedBackups(DELETE_AUTOMATED_BACKUPS_YES)
                .deletionProtection(DELETION_PROTECTION_NO)
                .domain(DOMAIN_EMPTY)
                .domainIAMRoleName(DOMAIN_IAM_ROLE_NAME_EMPTY)
                .enableCloudwatchLogsExports(ImmutableList.of())
                .enableIAMDatabaseAuthentication(ENABLE_IAM_DATABASE_AUTHENTICATION_NO)
                .enablePerformanceInsights(ENABLE_PERFORMANCE_INSIGHTS_NO)
                .engine(ENGINE_MYSQL)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .iops(IOPS_DEFAULT)
                .kmsKeyId(KMS_KEY_ID_EMPTY)
                .licenseModel(LICENSE_MODEL_LICENSE_INCLUDED)
                .masterUsername(MASTER_USERNAME)
                .masterUserPassword(MASTER_USER_PASSWORD)
                .maxAllocatedStorage(MAX_ALLOCATED_STORAGE_DEFAULT)
                .monitoringInterval(MONITORING_INTERVAL_DEFAULT)
                .monitoringRoleArn(MONITORING_ROLE_ARN)
                .multiAZ(MULTI_AZ_NO)
                .optionGroupName(OPTION_GROUP_NAME_MYSQL_DEFAULT)
                .performanceInsightsKMSKeyId(PERFORMANCE_INSIGHTS_KMS_ID_EMPTY)
                .performanceInsightsRetentionPeriod(PERFORMANCE_INSIGHTS_RETENTION_PERIOD_DEFAULT)
                .port(PORT_DEFAULT)
                .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_EMPTY)
                .preferredMaintenanceWindow(PREFERRED_MAINTENANCE_WINDOW_EMPTY)
                .processorFeatures(ImmutableList.of(PROCESSOR_FEATURE))
                .promotionTier(PROMOTION_TIER_DEFAULT)
                .publiclyAccessible(PUBLICLY_ACCESSIBLE_NO)
                .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_EMPTY)
                .sourceRegion(SOURCE_REGION_EMPTY)
                .storageEncrypted(STORAGE_ENCRYPTED_NO)
                .storageType(STORAGE_TYPE_STANDARD)
                .tags(TAG_LIST_ALTER)
                .timezone(TIMEZONE_DEFAULT)
                .useDefaultProcessorFeatures(USE_DEFAULT_PROCESSOR_FEATURES_YES)
                .vPCSecurityGroups(ImmutableList.of(VPC_SECURITY_GROUP_NAME_DEFAULT))
                .build();

        RESOURCE_MODEL_READ_REPLICA = ResourceModel.builder()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .associatedRoles(ASSOCIATED_ROLES)
                .autoMinorVersionUpgrade(AUTO_MINOR_VERSION_UPGRADE_YES)
                .availabilityZone(AVAILABILITY_ZONE)
                .backupRetentionPeriod(BACKUP_RETENTION_PERIOD_DEFAULT)
                .cACertificateIdentifier(CA_CERTIFICATE_IDENTIFIER_NON_EMPTY)
                .characterSetName(CHARACTER_SET_NAME)
                .copyTagsToSnapshot(COPY_TAGS_TO_SNAPSHOT_YES)
                .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_EMPTY)
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER_NON_EMPTY)
                .dBName(DB_NAME)
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                .dBSnapshotIdentifier(DB_SNAPSHOT_IDENTIFIER_EMPTY)
                .dBSubnetGroupName(DB_SUBNET_GROUP_NAME_DEFAULT)
                .deleteAutomatedBackups(DELETE_AUTOMATED_BACKUPS_YES)
                .deletionProtection(DELETION_PROTECTION_NO)
                .domain(DOMAIN_EMPTY)
                .domainIAMRoleName(DOMAIN_IAM_ROLE_NAME_EMPTY)
                .enableCloudwatchLogsExports(ImmutableList.of())
                .enableIAMDatabaseAuthentication(ENABLE_IAM_DATABASE_AUTHENTICATION_NO)
                .enablePerformanceInsights(ENABLE_PERFORMANCE_INSIGHTS_NO)
                .engine(ENGINE_MYSQL)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .iops(IOPS_DEFAULT)
                .kmsKeyId(KMS_KEY_ID_EMPTY)
                .licenseModel(LICENSE_MODEL_LICENSE_INCLUDED)
                .masterUsername(MASTER_USERNAME)
                .masterUserPassword(MASTER_USER_PASSWORD)
                .maxAllocatedStorage(MAX_ALLOCATED_STORAGE_DEFAULT)
                .monitoringInterval(MONITORING_INTERVAL_DEFAULT)
                .monitoringRoleArn(MONITORING_ROLE_ARN)
                .multiAZ(MULTI_AZ_NO)
                .optionGroupName(OPTION_GROUP_NAME_MYSQL_DEFAULT)
                .performanceInsightsKMSKeyId(PERFORMANCE_INSIGHTS_KMS_ID_EMPTY)
                .performanceInsightsRetentionPeriod(PERFORMANCE_INSIGHTS_RETENTION_PERIOD_DEFAULT)
                .port(PORT_DEFAULT)
                .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_EMPTY)
                .preferredMaintenanceWindow(PREFERRED_MAINTENANCE_WINDOW_EMPTY)
                .processorFeatures(ImmutableList.of(PROCESSOR_FEATURE))
                .promotionTier(PROMOTION_TIER_DEFAULT)
                .publiclyAccessible(PUBLICLY_ACCESSIBLE_NO)
                .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_NON_EMPTY) // setting this field marks the instance as read replica
                .sourceRegion(SOURCE_REGION_EMPTY)
                .storageEncrypted(STORAGE_ENCRYPTED_NO)
                .storageType(STORAGE_TYPE_STANDARD)
                .tags(TAG_LIST)
                .timezone(TIMEZONE_DEFAULT)
                .useDefaultProcessorFeatures(USE_DEFAULT_PROCESSOR_FEATURES_YES)
                .vPCSecurityGroups(ImmutableList.of(VPC_SECURITY_GROUP_NAME_DEFAULT))
                .build();

        RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT = ResourceModel.builder()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .associatedRoles(ASSOCIATED_ROLES)
                .autoMinorVersionUpgrade(AUTO_MINOR_VERSION_UPGRADE_YES)
                .availabilityZone(AVAILABILITY_ZONE)
                .backupRetentionPeriod(BACKUP_RETENTION_PERIOD_DEFAULT)
                .cACertificateIdentifier(CA_CERTIFICATE_IDENTIFIER_NON_EMPTY)
                .characterSetName(CHARACTER_SET_NAME)
                .copyTagsToSnapshot(COPY_TAGS_TO_SNAPSHOT_YES)
                .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_EMPTY)
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER_NON_EMPTY)
                .dBName(DB_NAME)
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                .dBSnapshotIdentifier(DB_SNAPSHOT_IDENTIFIER_NON_EMPTY) // setting this field marks the instance as recovering from the snapshot
                .dBSubnetGroupName(DB_SUBNET_GROUP_NAME_DEFAULT)
                .deleteAutomatedBackups(DELETE_AUTOMATED_BACKUPS_YES)
                .deletionProtection(DELETION_PROTECTION_NO)
                .domain(DOMAIN_EMPTY)
                .domainIAMRoleName(DOMAIN_IAM_ROLE_NAME_EMPTY)
                .enableCloudwatchLogsExports(ImmutableList.of())
                .enableIAMDatabaseAuthentication(ENABLE_IAM_DATABASE_AUTHENTICATION_NO)
                .enablePerformanceInsights(ENABLE_PERFORMANCE_INSIGHTS_NO)
                .engine(ENGINE_MYSQL)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .iops(IOPS_DEFAULT)
                .kmsKeyId(KMS_KEY_ID_EMPTY)
                .licenseModel(LICENSE_MODEL_LICENSE_INCLUDED)
                .masterUsername(MASTER_USERNAME)
                .masterUserPassword(MASTER_USER_PASSWORD)
                .maxAllocatedStorage(MAX_ALLOCATED_STORAGE_DEFAULT)
                .monitoringInterval(MONITORING_INTERVAL_DEFAULT)
                .monitoringRoleArn(MONITORING_ROLE_ARN)
                .multiAZ(MULTI_AZ_NO)
                .optionGroupName(OPTION_GROUP_NAME_MYSQL_DEFAULT)
                .performanceInsightsKMSKeyId(PERFORMANCE_INSIGHTS_KMS_ID_EMPTY)
                .performanceInsightsRetentionPeriod(PERFORMANCE_INSIGHTS_RETENTION_PERIOD_DEFAULT)
                .port(PORT_DEFAULT)
                .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_EMPTY)
                .preferredMaintenanceWindow(PREFERRED_MAINTENANCE_WINDOW_EMPTY)
                .processorFeatures(ImmutableList.of(PROCESSOR_FEATURE))
                .promotionTier(PROMOTION_TIER_DEFAULT)
                .publiclyAccessible(PUBLICLY_ACCESSIBLE_NO)
                .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_EMPTY)
                .sourceRegion(SOURCE_REGION_EMPTY)
                .storageEncrypted(STORAGE_ENCRYPTED_NO)
                .storageType(STORAGE_TYPE_STANDARD)
                .tags(TAG_LIST)
                .timezone(TIMEZONE_DEFAULT)
                .useDefaultProcessorFeatures(USE_DEFAULT_PROCESSOR_FEATURES_YES)
                .vPCSecurityGroups(ImmutableList.of(VPC_SECURITY_GROUP_NAME_DEFAULT))
                .build();

        DB_INSTANCE_BASE = DBInstance.builder()
                .dbInstanceIdentifier(DB_INSTANCE_IDENTIFIER_NON_EMPTY)
                .dbName(DB_NAME)
                .dbInstanceArn(DB_INSTANCE_ARN_NON_EMPTY)
                .engine(ENGINE_MYSQL)
                .dbInstancePort(PORT_DEFAULT)
                .dbInstanceStatus(DB_INSTANCE_STATUS_AVAILABLE)
                .build();

        DB_INSTANCE_ACTIVE = DBInstance.builder()
                .allocatedStorage(ALLOCATED_STORAGE)
                .autoMinorVersionUpgrade(AUTO_MINOR_VERSION_UPGRADE_YES)
                .availabilityZone(AVAILABILITY_ZONE)
                .backupRetentionPeriod(BACKUP_RETENTION_PERIOD_DEFAULT)
                .caCertificateIdentifier(CA_CERTIFICATE_IDENTIFIER_EMPTY)
                .characterSetName(CHARACTER_SET_NAME)
                .copyTagsToSnapshot(COPY_TAGS_TO_SNAPSHOT_YES)
                .dbInstanceIdentifier(DB_INSTANCE_IDENTIFIER_NON_EMPTY)
                .dbClusterIdentifier(DB_CLUSTER_IDENTIFIER_EMPTY)
                .dbInstanceArn(DB_INSTANCE_ARN_NON_EMPTY)
                .dbInstancePort(PORT_DEFAULT)
                .dbName(DB_NAME)
                .dbInstanceClass(DB_INSTANCE_CLASS_DEFAULT)
                .dbInstanceStatus(DB_INSTANCE_STATUS_AVAILABLE)
                .deletionProtection(DELETION_PROTECTION_NO)
                .engine(ENGINE_MYSQL)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .iops(IOPS_DEFAULT)
                .kmsKeyId(KMS_KEY_ID_EMPTY)
                .licenseModel(LICENSE_MODEL_GENERAL_PUBLIC_LICENSE)
                .iamDatabaseAuthenticationEnabled(ENABLE_IAM_DATABASE_AUTHENTICATION_YES)
                .promotionTier(PROMOTION_TIER_DEFAULT)
                .storageType(STORAGE_TYPE_STANDARD)
                .storageEncrypted(STORAGE_ENCRYPTED_NO)
                .masterUsername(MASTER_USERNAME)
                .build();

        DB_INSTANCE_DELETING = DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceStatus(DB_INSTANCE_STATUS_DELETING)
                .build();

        DB_INSTANCE_MODIFYING = DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceStatus(DB_INSTANCE_STATUS_MODIFYING)
                .build();

        DB_INSTANCE_EMPTY_PORT = DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstancePort(null)
                .endpoint(
                        Endpoint.builder()
                                .port(PORT_DEFAULT)
                                .build()
                )
                .build();
    }

    static ResourceModel.ResourceModelBuilder RESOURCE_MODEL_BAREBONE_BLDR() {
        return ResourceModel.builder()
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER_NON_EMPTY);
    }

    static ResourceModel.ResourceModelBuilder RESOURCE_MODEL_BLDR() {
        return ResourceModel.builder()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .associatedRoles(ASSOCIATED_ROLES)
                .autoMinorVersionUpgrade(AUTO_MINOR_VERSION_UPGRADE_YES)
                .availabilityZone(AVAILABILITY_ZONE)
                .backupRetentionPeriod(BACKUP_RETENTION_PERIOD_DEFAULT)
                .cACertificateIdentifier(CA_CERTIFICATE_IDENTIFIER_NON_EMPTY)
                .characterSetName(CHARACTER_SET_NAME)
                .copyTagsToSnapshot(COPY_TAGS_TO_SNAPSHOT_YES)
                .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER_EMPTY)
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER_NON_EMPTY)
                .dBName(DB_NAME)
                .dBParameterGroupName(DB_PARAMETER_GROUP_NAME_DEFAULT)
                .dBSnapshotIdentifier(DB_SNAPSHOT_IDENTIFIER_EMPTY)
                .dBSubnetGroupName(DB_SUBNET_GROUP_NAME_DEFAULT)
                .deleteAutomatedBackups(DELETE_AUTOMATED_BACKUPS_YES)
                .deletionProtection(DELETION_PROTECTION_NO)
                .domain(DOMAIN_EMPTY)
                .domainIAMRoleName(DOMAIN_IAM_ROLE_NAME_EMPTY)
                .enableCloudwatchLogsExports(ImmutableList.of())
                .enableIAMDatabaseAuthentication(ENABLE_IAM_DATABASE_AUTHENTICATION_NO)
                .enablePerformanceInsights(ENABLE_PERFORMANCE_INSIGHTS_NO)
                .engine(ENGINE_MYSQL)
                .engineVersion(ENGINE_VERSION_MYSQL_56)
                .iops(IOPS_DEFAULT)
                .kmsKeyId(KMS_KEY_ID_EMPTY)
                .licenseModel(LICENSE_MODEL_LICENSE_INCLUDED)
                .masterUsername(MASTER_USERNAME)
                .masterUserPassword(MASTER_USER_PASSWORD)
                .maxAllocatedStorage(MAX_ALLOCATED_STORAGE_DEFAULT)
                .monitoringInterval(MONITORING_INTERVAL_DEFAULT)
                .monitoringRoleArn(MONITORING_ROLE_ARN)
                .multiAZ(MULTI_AZ_NO)
                .optionGroupName(OPTION_GROUP_NAME_MYSQL_DEFAULT)
                .performanceInsightsKMSKeyId(PERFORMANCE_INSIGHTS_KMS_ID_EMPTY)
                .performanceInsightsRetentionPeriod(PERFORMANCE_INSIGHTS_RETENTION_PERIOD_DEFAULT)
                .port(PORT_DEFAULT)
                .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_EMPTY)
                .preferredMaintenanceWindow(PREFERRED_MAINTENANCE_WINDOW_EMPTY)
                .processorFeatures(ImmutableList.of(PROCESSOR_FEATURE))
                .promotionTier(PROMOTION_TIER_DEFAULT)
                .publiclyAccessible(PUBLICLY_ACCESSIBLE_NO)
                .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_IDENTIFIER_EMPTY)
                .sourceRegion(SOURCE_REGION_EMPTY)
                .storageEncrypted(STORAGE_ENCRYPTED_NO)
                .storageType(STORAGE_TYPE_STANDARD)
                .tags(TAG_LIST)
                .timezone(TIMEZONE_DEFAULT)
                .useDefaultProcessorFeatures(USE_DEFAULT_PROCESSOR_FEATURES_YES)
                .vPCSecurityGroups(ImmutableList.of(VPC_SECURITY_GROUP_NAME_DEFAULT));
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
    protected void expectResourceSupply(final Supplier<DBInstance> supplier) {
        when(getRdsProxy()
                .client()
                .describeDBInstances(any(DescribeDbInstancesRequest.class))
        ).then(res -> DescribeDbInstancesResponse.builder()
                .dbInstances(supplier.get())
                .build()
        );
    }

    // This helper method computes DBInstance state transitions upon an assigned roles update.
    // The roles are being removed and added one-by-one, resulting in the transition progression.
    // Assume an instance has 2 roles: [A, B]
    // The new request comes in as: [C, D, E]
    // The method will generate a list of nearly-identical DBInstances with distinct associated roles:
    // [
    //      [A, B],
    //      [B],
    //      [],
    //      [C],
    //      [C, D]
    // ]
    // Which literally stands for:
    // 1. remove role A, expect role be to be present
    // 2. remove role B, expect no roles to be present
    // 3. add role C, expect role C to be present
    // 4. add role D, expect roles C and D to be present
    protected List<DBInstance> computeAssociatedRoleTransitions(
            final DBInstance dbInstance,
            final List<DBInstanceRole> initialRoles,
            final List<DBInstanceRole> finalRoles
    ) {
        final List<DBInstance> result = new LinkedList<>();

        for (int i = 0; i < initialRoles.size(); i++) {
            result.add(dbInstance.toBuilder()
                    .associatedRoles(Translator.translateAssociatedRolesToSdk(
                            initialRoles.subList(i, initialRoles.size())
                    )).build());
        }

        // Less or equal is not a mistake if you compare the loop invariant to the previous one:
        // one of these loops should generate an empty subset of associated roles in the middle,
        // in this case the second loop does it.
        for (int i = 0; i <= finalRoles.size(); i++) {
            result.add(dbInstance.toBuilder()
                    .associatedRoles(Translator.translateAssociatedRolesToSdk(
                            finalRoles.subList(0, i)
                    )).build());
        }

        return result;
    }
}
