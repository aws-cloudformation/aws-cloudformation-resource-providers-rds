package software.amazon.rds.dbinstance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static software.amazon.rds.dbinstance.BaseHandlerStd.API_VERSION_V12;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import org.json.JSONObject;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaResponse;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.Endpoint;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.dbinstance.client.ApiVersion;
import software.amazon.rds.dbinstance.client.VersionedProxyClient;
import software.amazon.rds.test.common.core.AbstractTestBase;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.MethodCallExpectation;
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBInstance, ResourceModel, CallbackContext> {

    protected static final String LOGICAL_RESOURCE_IDENTIFIER = "dbinstance";

    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;

    protected static final List<Tag> TAG_LIST_EMPTY;
    protected static final List<Tag> TAG_LIST;
    protected static final List<Tag> TAG_LIST_ALTER;
    protected static final Tagging.TagSet TAG_SET;

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
    protected static final String DB_INSTANCE_STATUS_STORAGE_FULL = "storage-full";
    protected static final String DB_NAME = "db-instance-db-name";
    protected static final String DB_PARAMETER_GROUP_NAME_DEFAULT = "default";
    protected static final String DB_PARAMETER_GROUP_NAME_ALTER = "alternative-parameter-group";
    protected static final String DB_SECURITY_GROUP_DEFAULT = "default";
    protected static final String DB_SECURITY_GROUP_ID = "db-security-group-id";
    protected static final String DB_SECURITY_GROUP_VPC_ID = "db-security-group-vpc-id";
    protected static final List<String> DB_SECURITY_GROUPS;
    protected static final List<String> DB_SECURITY_GROUPS_ALTER;

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
    protected static final Integer PORT_DEFAULT_INT = 3306;
    protected static final String PORT_DEFAULT_STR = PORT_DEFAULT_INT.toString();
    protected static final String PREFERRED_BACKUP_WINDOW_EMPTY = null;
    protected static final String PREFERRED_BACKUP_WINDOW_NON_EMPTY = "03:00–11:00";
    protected static final String PREFERRED_MAINTENANCE_WINDOW_NON_EMPTY = "03:00–11:00";
    protected static final String PREFERRED_MAINTENANCE_WINDOW_EMPTY = null;
    protected static final String PROCESSOR_FEATURE_NAME = "processor-feature-name";
    protected static final String PROCESSOR_FEATURE_VALUE = "processor-feature-value";
    protected static final List<ProcessorFeature> PROCESSOR_FEATURES;
    protected static final List<ProcessorFeature> PROCESSOR_FEATURES_ALTER;
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
    protected static final String MSG_GENERIC_ERR = "Error";

    protected static final ResourceModel RESOURCE_MODEL_NO_IDENTIFIER;
    protected static final ResourceModel RESOURCE_MODEL_ALTER;
    protected static final ResourceModel RESOURCE_MODEL_READ_REPLICA;
    protected static final ResourceModel RESOURCE_MODEL_RESTORING_FROM_SNAPSHOT;

    protected static final DBInstance DB_INSTANCE_BASE;
    protected static final DBInstance DB_INSTANCE_ACTIVE;
    protected static final DBInstance DB_INSTANCE_DELETING;
    protected static final DBInstance DB_INSTANCE_MODIFYING;
    protected static final DBInstance DB_INSTANCE_EMPTY_PORT;
    protected static final DBInstance DB_INSTANCE_STORAGE_FULL;

    protected static Constant TEST_BACKOFF_DELAY = Constant.of()
            .delay(Duration.ofMillis(1L))
            .timeout(Duration.ofSeconds(10L))
            .build();

    static {
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
        delegate = LoggerFactory.getLogger("testing");
        logger = new LoggerProxy();

        TAG_LIST_EMPTY = ImmutableList.of();
        TAG_LIST = ImmutableList.of(
                Tag.builder().key("foo-1").value("bar-1").build(),
                Tag.builder().key("foo-2").value("bar-2").build(),
                Tag.builder().key("foo-3").value("bar-3").build()
        );

        TAG_LIST_ALTER = ImmutableList.of(
                Tag.builder().key("foo-4").value("bar-4").build(),
                Tag.builder().key("foo-5").value("bar-5").build()
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

        ASSOCIATED_ROLES = ImmutableList.of(
                DBInstanceRole.builder()
                        .featureName(ASSOCIATED_ROLE_NAME + "-1")
                        .roleArn(ASSOCIATED_ROLE_ARN + "-1")
                        .build(),
                DBInstanceRole.builder()
                        .featureName(ASSOCIATED_ROLE_NAME + "-2")
                        .roleArn(ASSOCIATED_ROLE_ARN + "-2")
                        .build(),
                DBInstanceRole.builder()
                        .featureName(ASSOCIATED_ROLE_NAME + "-3")
                        .roleArn(ASSOCIATED_ROLE_ARN + "-3")
                        .build()
        );
        ASSOCIATED_ROLES_ALTER = ImmutableList.of(
                DBInstanceRole.builder()
                        .featureName(ASSOCIATED_ROLE_NAME + "-4")
                        .roleArn(ASSOCIATED_ROLE_ARN + "-4")
                        .build(),
                DBInstanceRole.builder()
                        .featureName(ASSOCIATED_ROLE_NAME + "-5")
                        .roleArn(ASSOCIATED_ROLE_ARN + "-5")
                        .build()
        );

        PROCESSOR_FEATURES = ImmutableList.of(
                ProcessorFeature.builder()
                        .name(PROCESSOR_FEATURE_NAME + "-1")
                        .value(PROCESSOR_FEATURE_VALUE + "-1")
                        .build(),
                ProcessorFeature.builder()
                        .name(PROCESSOR_FEATURE_NAME + "-2")
                        .value(PROCESSOR_FEATURE_VALUE + "-2")
                        .build(),
                ProcessorFeature.builder()
                        .name(PROCESSOR_FEATURE_NAME + "-3")
                        .value(PROCESSOR_FEATURE_VALUE + "-3")
                        .build()
        );

        PROCESSOR_FEATURES_ALTER = ImmutableList.of(
                ProcessorFeature.builder()
                        .name(PROCESSOR_FEATURE_NAME + "-4")
                        .value(PROCESSOR_FEATURE_VALUE + "-4")
                        .build(),
                ProcessorFeature.builder()
                        .name(PROCESSOR_FEATURE_NAME + "-5")
                        .value(PROCESSOR_FEATURE_VALUE + "-5")
                        .build()
        );

        DB_SECURITY_GROUPS = ImmutableList.of("db-sec-group-1", "db-sec-group-2");
        DB_SECURITY_GROUPS_ALTER = ImmutableList.of("db-sec-group-1", "db-sec-group-2", "db-sec-group-3");


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
                .port(PORT_DEFAULT_STR)
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
                .port(PORT_DEFAULT_STR)
                .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_EMPTY)
                .preferredMaintenanceWindow(PREFERRED_MAINTENANCE_WINDOW_EMPTY)
                .processorFeatures(PROCESSOR_FEATURES)
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
                .port(PORT_DEFAULT_STR)
                .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_EMPTY)
                .preferredMaintenanceWindow(PREFERRED_MAINTENANCE_WINDOW_EMPTY)
                .processorFeatures(PROCESSOR_FEATURES)
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
                .port(PORT_DEFAULT_STR)
                .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_EMPTY)
                .preferredMaintenanceWindow(PREFERRED_MAINTENANCE_WINDOW_EMPTY)
                .processorFeatures(PROCESSOR_FEATURES)
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
                .dbInstancePort(PORT_DEFAULT_INT)
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
                .dbInstancePort(PORT_DEFAULT_INT)
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
                .storageType(STORAGE_TYPE_STANDARD)
                .storageEncrypted(STORAGE_ENCRYPTED_NO)
                .masterUsername(MASTER_USERNAME)
                .promotionTier(PROMOTION_TIER_DEFAULT)
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
                                .port(PORT_DEFAULT_INT)
                                .build()
                )
                .build();

        DB_INSTANCE_STORAGE_FULL = DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceStatus(DB_INSTANCE_STATUS_STORAGE_FULL)
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
                .port(PORT_DEFAULT_STR)
                .preferredBackupWindow(PREFERRED_BACKUP_WINDOW_EMPTY)
                .preferredMaintenanceWindow(PREFERRED_MAINTENANCE_WINDOW_EMPTY)
                .processorFeatures(PROCESSOR_FEATURES)
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

    static <ClientT> ProxyClient<ClientT> mockProxy(final AmazonWebServicesClientProxy proxy, final ClientT client) {
        return new BaseProxyClient<>(proxy, client);
    }

    static <ClientT> VersionedProxyClient<ClientT> mockVersionedProxy(final AmazonWebServicesClientProxy proxy, final ClientT client) {
        return new VersionedProxyClient<ClientT>()
                .register(ApiVersion.DEFAULT, new BaseProxyClient<ClientT>(proxy, client));
    }

    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    protected ProxyClient<RdsClient> getRdsProxy(final String version) {
        return getRdsProxy();
    }

    protected abstract ProxyClient<Ec2Client> getEc2Proxy();

    public abstract HandlerName getHandlerName();

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    public void verifyAccessPermissions(final Object mock) {
        new AccessPermissionVerificationMode()
                .withDefaultPermissions()
                .withSchemaPermissions(resourceSchema, getHandlerName())
                .verify(TestUtils.getVerificationData(mock));
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context
    ) {
        return getHandler().handleRequest(
                getProxy(),
                request,
                context,
                new VersionedProxyClient<RdsClient>()
                        .register(ApiVersion.V12, getRdsProxy(API_VERSION_V12))
                        .register(ApiVersion.DEFAULT, getRdsProxy()),
                new VersionedProxyClient<Ec2Client>().register(ApiVersion.DEFAULT, getEc2Proxy()),
                logger
        );
    }

    @Override
    protected String getLogicalResourceIdentifier() {
        return LOGICAL_RESOURCE_IDENTIFIER;
    }

    @Override
    protected void expectResourceSupply(final Supplier<DBInstance> supplier) {
        expectDescribeDBInstancesCall().setup()
                .then(res -> DescribeDbInstancesResponse.builder()
                        .dbInstances(supplier.get())
                        .build());
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

    protected MethodCallExpectation<DescribeDbInstancesRequest, DescribeDbInstancesResponse> expectDescribeDBInstancesCall() {
        return new MethodCallExpectation<DescribeDbInstancesRequest, DescribeDbInstancesResponse>() {
            @Override
            public OngoingStubbing<DescribeDbInstancesResponse> setup() {
                return when(getRdsProxy().client().describeDBInstances(any(DescribeDbInstancesRequest.class)));
            }

            @Override
            public ArgumentCaptor<DescribeDbInstancesRequest> verify() {
                final ArgumentCaptor<DescribeDbInstancesRequest> captor = ArgumentCaptor.forClass(DescribeDbInstancesRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).describeDBInstances(captor.capture());
                return captor;
            }
        };
    }

    protected MethodCallExpectation<CreateDbInstanceRequest, CreateDbInstanceResponse> expectCreateDBInstanceCall() {
        return new MethodCallExpectation<CreateDbInstanceRequest, CreateDbInstanceResponse>() {
            @Override
            public OngoingStubbing<CreateDbInstanceResponse> setup() {
                return when(getRdsProxy().client().createDBInstance(any(CreateDbInstanceRequest.class)));
            }

            @Override
            public ArgumentCaptor<CreateDbInstanceRequest> verify() {
                final ArgumentCaptor<CreateDbInstanceRequest> captor = ArgumentCaptor.forClass(CreateDbInstanceRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).createDBInstance(captor.capture());
                return captor;
            }
        };
    }

    protected MethodCallExpectation<CreateDbInstanceReadReplicaRequest, CreateDbInstanceReadReplicaResponse> expectCreateDBInstanceReadReplicaCall() {
        return new MethodCallExpectation<CreateDbInstanceReadReplicaRequest, CreateDbInstanceReadReplicaResponse>() {
            @Override
            public OngoingStubbing<CreateDbInstanceReadReplicaResponse> setup() {
                return when(getRdsProxy().client().createDBInstanceReadReplica(any(CreateDbInstanceReadReplicaRequest.class)));
            }

            @Override
            public ArgumentCaptor<CreateDbInstanceReadReplicaRequest> verify() {
                final ArgumentCaptor<CreateDbInstanceReadReplicaRequest> captor = ArgumentCaptor.forClass(CreateDbInstanceReadReplicaRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).createDBInstanceReadReplica(captor.capture());
                return captor;
            }
        };
    }

    protected MethodCallExpectation<RestoreDbInstanceFromDbSnapshotRequest, RestoreDbInstanceFromDbSnapshotResponse> expectRestoreDBInstanceFromDBSnapshotCall() {
        return new MethodCallExpectation<RestoreDbInstanceFromDbSnapshotRequest, RestoreDbInstanceFromDbSnapshotResponse>() {
            @Override
            public OngoingStubbing<RestoreDbInstanceFromDbSnapshotResponse> setup() {
                return when(getRdsProxy().client().restoreDBInstanceFromDBSnapshot(any(RestoreDbInstanceFromDbSnapshotRequest.class)));
            }

            @Override
            public ArgumentCaptor<RestoreDbInstanceFromDbSnapshotRequest> verify() {
                final ArgumentCaptor<RestoreDbInstanceFromDbSnapshotRequest> captor = ArgumentCaptor.forClass(RestoreDbInstanceFromDbSnapshotRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).restoreDBInstanceFromDBSnapshot(captor.capture());
                return captor;
            }
        };
    }

    protected MethodCallExpectation<ModifyDbInstanceRequest, ModifyDbInstanceResponse> expectModifyDBInstanceCall() {
        return new MethodCallExpectation<ModifyDbInstanceRequest, ModifyDbInstanceResponse>() {
            @Override
            public OngoingStubbing<ModifyDbInstanceResponse> setup() {
                return when(getRdsProxy().client().modifyDBInstance(any(ModifyDbInstanceRequest.class)));
            }

            @Override
            public ArgumentCaptor<ModifyDbInstanceRequest> verify() {
                final ArgumentCaptor<ModifyDbInstanceRequest> captor = ArgumentCaptor.forClass(ModifyDbInstanceRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).modifyDBInstance(captor.capture());
                return captor;
            }
        };
    }

    protected MethodCallExpectation<DeleteDbInstanceRequest, DeleteDbInstanceResponse> expectDeleteDBInstanceCall() {
        return new MethodCallExpectation<DeleteDbInstanceRequest, DeleteDbInstanceResponse>() {
            @Override
            public OngoingStubbing<DeleteDbInstanceResponse> setup() {
                return when(getRdsProxy().client().deleteDBInstance(any(DeleteDbInstanceRequest.class)));
            }

            @Override
            public ArgumentCaptor<DeleteDbInstanceRequest> verify() {
                final ArgumentCaptor<DeleteDbInstanceRequest> captor = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).deleteDBInstance(captor.capture());
                return captor;
            }
        };
    }
}
