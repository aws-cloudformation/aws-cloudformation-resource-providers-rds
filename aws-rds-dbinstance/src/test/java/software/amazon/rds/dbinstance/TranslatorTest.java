package software.amazon.rds.dbinstance;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceReadReplicaRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DomainMembership;
import software.amazon.awssdk.services.rds.model.Endpoint;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.OptionGroupMembership;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceToPointInTimeRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;

class TranslatorTest extends AbstractHandlerTest {

    @Test
    public void test_modifyDbInstanceRequest_IncreaseAllocatedStorage() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE_INCR.toString())
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_INCR);
    }

    @Test
    public void test_modifyDbInstanceRequestV12_IncreaseAllocatedStorage() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE_INCR.toString())
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previousModel, desiredModel, isRollback);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_INCR);
    }

    @Test
    public void test_modifyDbInstanceRequest_DecreaseAllocatedStorage() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE_DECR.toString())
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_DECR);
    }

    @Test
    public void test_modifyDbInstanceRequestV12_DecreaseAllocatedStorage() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE_DECR.toString())
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previousModel, desiredModel, isRollback);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_DECR);
    }

    @Test
    public void test_modifyDbInstanceRequest_isRollback_IncreaseAllocatedStorage() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE_INCR.toString())
                .build();
        final Boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequest_isRollback_NoAllocatedStorage() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(null)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(null)
                .storageType(STORAGE_TYPE_IO1)
                .build();
        final Boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequestV12_isRollback_IncreaseAllocatedStorage() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE_INCR.toString())
                .build();
        final Boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previousModel, desiredModel, isRollback);
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequest_isRollback_DecreaseAllocatedStorage() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE_DECR.toString())
                .build();
        final Boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequestV12_isRollback_DecreaseAllocatedStorage() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE.toString())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .allocatedStorage(ALLOCATED_STORAGE_DECR.toString())
                .build();
        final Boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previousModel, desiredModel, isRollback);
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequest_IncreaseIops() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DEFAULT)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_INCR)
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.iops()).isEqualTo(IOPS_INCR);
    }

    @Test
    public void test_modifyDbInstanceRequestV12_IncreaseIops() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DEFAULT)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_INCR)
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previousModel, desiredModel, isRollback);
        assertThat(request.iops()).isEqualTo(IOPS_INCR);
    }

    @Test
    public void test_modifyDbInstanceRequest_DecreaseIops() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DEFAULT)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DECR)
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.iops()).isEqualTo(IOPS_DECR);
    }

    @Test
    public void test_modifyDbInstanceRequestV12_DecreaseIops() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DEFAULT)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DECR)
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previousModel, desiredModel, isRollback);
        assertThat(request.iops()).isEqualTo(IOPS_DECR);
    }

    @Test
    public void test_modifyDbInstanceRequest_isRollback_IncreaseIops() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DEFAULT)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_INCR)
                .build();
        final Boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.iops()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequestV12_isRollback_IncreaseIops() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DEFAULT)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_INCR)
                .build();
        final Boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previousModel, desiredModel, isRollback);
        assertThat(request.iops()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequest_isRollback_DecreaseIops() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DEFAULT)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DECR)
                .build();
        final Boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.iops()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequestV12_isRollback_DecreaseIops() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DEFAULT)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .iops(IOPS_DECR)
                .build();
        final Boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previousModel, desiredModel, isRollback);
        assertThat(request.iops()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequest_setUseDefaultProcessorFeatures() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .useDefaultProcessorFeatures(false)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .useDefaultProcessorFeatures(true)
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.useDefaultProcessorFeatures()).isTrue();
    }

    @Test
    public void test_modifyDbInstanceRequest_setProcessorFeatures() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .processorFeatures(PROCESSOR_FEATURES)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .processorFeatures(PROCESSOR_FEATURES_ALTER)
                .build();
        final Boolean isRollback = false;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.processorFeatures()).hasSameElementsAs(Translator.translateProcessorFeaturesToSdk(PROCESSOR_FEATURES_ALTER));
    }

    @Test
    public void test_createDBInstanceRequest_stableHashCode() {
        final Tagging.TagSet tagSet1 = Tagging.TagSet.builder()
                .systemTags(Translator.translateTagsToSdk(TAG_LIST))
                .resourceTags(Translator.translateTagsToSdk(TAG_LIST))
                .stackTags(Translator.translateTagsToSdk(TAG_LIST))
                .build();
        final Tagging.TagSet tagSet2 = Tagging.TagSet.builder()
                .systemTags(Translator.translateTagsToSdk(TAG_LIST))
                .resourceTags(Translator.translateTagsToSdk(TAG_LIST))
                .stackTags(Translator.translateTagsToSdk(TAG_LIST))
                .build();
        final CreateDbInstanceRequest request1 = Translator.createDbInstanceRequest(RESOURCE_MODEL_BLDR().build(), tagSet1);
        final CreateDbInstanceRequest request2 = Translator.createDbInstanceRequest(RESOURCE_MODEL_BLDR().build(), tagSet2);

        Assertions.assertEquals(request1.hashCode(), request2.hashCode());
    }

    @Test
    public void test_createReadReplicaRequest_parameterGroupSetForMySql() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .sourceDBInstanceIdentifier(SOURCE_DB_INSTANCE_ARN)
                .build();

        final CreateDbInstanceReadReplicaRequest request = Translator.createDbInstanceReadReplicaRequest(model, Tagging.TagSet.builder().build(), CURRENT_REGION);
        Assertions.assertNotNull(request.dbParameterGroupName());
    }

    @Test
    public void test_createReadReplicaRequest_parameterGroupNotSetForSqlServer() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .engine(ENGINE_SQLSERVER_SE)
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp3")
                .iops(100)
                .storageThroughput(200)
                .allocatedStorage("300")
                .build();

        final CreateDbInstanceReadReplicaRequest request = Translator.createDbInstanceReadReplicaRequest(model, Tagging.TagSet.builder().build(), CURRENT_REGION);
        Assertions.assertNull(request.dbParameterGroupName());
    }

    @Test
    public void test_createReadReplicaRequest_blankSourceRegionIsNotSet() {
        final ResourceModel model = ResourceModel.builder()
                .sourceRegion("")
                .build();
        final CreateDbInstanceReadReplicaRequest request = Translator.createDbInstanceReadReplicaRequest(model, Tagging.TagSet.builder().build(), CURRENT_REGION);
        Assertions.assertNull(request.sourceRegion());
    }

    @Test
    public void test_createReadReplicaRequest_nonBlankSourceRegionIsSet() {
        final String sourceRegion = "source-region";
        final ResourceModel model = ResourceModel.builder()
                .sourceRegion(sourceRegion)
                .build();
        final CreateDbInstanceReadReplicaRequest request = Translator.createDbInstanceReadReplicaRequest(model, Tagging.TagSet.builder().build(), CURRENT_REGION);
        Assertions.assertEquals(sourceRegion, request.sourceRegion());
    }

    @Test
    public void test_modifyReadReplicaRequest_parameterGroupNotSet() {
        final ResourceModel model = RESOURCE_MODEL_BLDR().build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(ResourceModel.builder().build(), model, false);
        Assertions.assertEquals("default", request.dbParameterGroupName());
    }

    @Test
    public void test_restoreFromSqlServerSnapshotRequest_storageType_shouldBeSetOnUpdate() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .engine(ENGINE_SQLSERVER_SE)
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp3")
                .iops(100)
                .storageThroughput(200)
                .allocatedStorage("300")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isNull();
        assertThat(request.iops()).isNull();
        assertThat(request.storageThroughput()).isNull();
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void test_restoreFromAuroraPostgresSnapshotRequest_storageType_shouldBeSetOnUpdate() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .engine(ENGINE_AURORA_POSTGRESQL)
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp3")
                .iops(100)
                .storageThroughput(200)
                .allocatedStorage("300")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isEqualTo(STORAGE_TYPE_GP3);
        assertThat(request.iops()).isEqualTo(100);
        assertThat(request.storageThroughput()).isEqualTo(200);
        assertThat(request.allocatedStorage()).isEqualTo(300);
    }

    @Test
    public void test_restoreDbInstanceToPointInTimeSqlServerRequest_shouldNotBeSetOnCreate() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .engine(ENGINE_SQLSERVER_EE)
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp3")
                .iops(100)
                .storageThroughput(200)
                .allocatedStorage("300")
                .build();

        final RestoreDbInstanceToPointInTimeRequest request = Translator.restoreDbInstanceToPointInTimeRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isNull();
        assertThat(request.iops()).isNull();
        assertThat(request.storageThroughput()).isNull();
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void test_restoreDbInstanceToPointInTimeAuroraPostgresRequest_shouldNotBeSetOnCreate() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .engine(ENGINE_AURORA_POSTGRESQL)
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp3")
                .iops(100)
                .storageThroughput(200)
                .allocatedStorage("300")
                .build();

        final RestoreDbInstanceToPointInTimeRequest request = Translator.restoreDbInstanceToPointInTimeRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isEqualTo(STORAGE_TYPE_GP3);
        assertThat(request.iops()).isEqualTo(100);
        assertThat(request.storageThroughput()).isEqualTo(200);
        assertThat(request.allocatedStorage()).isEqualTo(300);
    }

    @Test
    public void test_restoreFromSnapshotRequestV12_shouldNotBeSetOnRestore() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp3")
                .iops(100)
                .storageThroughput(200)
                .allocatedStorage("300")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequestV12(model);
        assertThat(request.storageType()).isNull();
        assertThat(request.iops()).isNull();
        assertThat(request.storageThroughput()).isNull();
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void test_restoreFromAuroraPostgresSnapshotRequest_storageTypeGp2() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .engine(ENGINE_AURORA_POSTGRESQL)
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isEqualTo(STORAGE_TYPE_GP2);
    }

    @Test
    public void test_restoreFromSqlServerSnapshotRequest_storageTypeGp2() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .engine(ENGINE_SQLSERVER_SE)
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isNull();
    }

    @Test
    public void test_restoreAuroraPostgresDbInstanceToPointInTimeRequest_storageTypeGp2() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .engine(ENGINE_AURORA_POSTGRESQL)
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        final RestoreDbInstanceToPointInTimeRequest request = Translator.restoreDbInstanceToPointInTimeRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isEqualTo(STORAGE_TYPE_GP2);
    }

    @Test
    public void test_restoreSqlServerDbInstanceToPointInTimeRequest_storageTypeGp2() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .engine(ENGINE_SQLSERVER_EX)
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        final RestoreDbInstanceToPointInTimeRequest request = Translator.restoreDbInstanceToPointInTimeRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isNull();
    }

    @Test
    public void test_restoreFromSnapshotRequestV12_storageTypeGp2() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequestV12(model);
        assertThat(request.storageType()).isNull();
    }

    @Test
    public void test_modifyDBInstanceRequest_cloudwatchLogsExportConfiguration_unchanged() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .enableCloudwatchLogsExports(ImmutableList.of("config-1", "config-2"))
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .enableCloudwatchLogsExports(ImmutableList.of("config-1", "config-2"))
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.cloudwatchLogsExportConfiguration()).isNull();
    }

    @Test
    public void test_modifyDBInstanceRequest_cloudwatchLogsExportConfiguration_changed() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .enableCloudwatchLogsExports(ImmutableList.of("config-1", "config-2"))
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .enableCloudwatchLogsExports(ImmutableList.of("config-1", "config-3"))
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.cloudwatchLogsExportConfiguration().disableLogTypes()).isEqualTo(ImmutableList.of("config-2"));
        assertThat(request.cloudwatchLogsExportConfiguration().enableLogTypes()).isEqualTo(ImmutableList.of("config-3"));
    }

    @Test
    public void test_modifyDBInstanceRequest_cloudwatchLogsExportConfiguration_previousNull() {
        final ResourceModel previousModel = RESOURCE_MODEL_BLDR()
                .enableCloudwatchLogsExports(null)
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL_BLDR()
                .enableCloudwatchLogsExports(ImmutableList.of("config-1", "config-2"))
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.cloudwatchLogsExportConfiguration().enableLogTypes()).isEqualTo(ImmutableList.of("config-1", "config-2"));
    }

    @Test
    public void test_translateDbInstanceFromSdkBuilder_emptyDomainMembership() {
        final DBInstance instance = DBInstance.builder()
                .domainMemberships((Collection<DomainMembership>) null)
                .build();
        final ResourceModel model = Translator.translateDbInstanceFromSdk(instance);
        assertThat(model.getDomain()).isNull();
        assertThat(model.getDomainIAMRoleName()).isNull();
    }

    @Test
    public void test_translateDbInstanceFromSdkBuilder_withDomainMembership() {
        final DBInstance instance = DBInstance.builder()
                .domainMemberships(ImmutableList.of(
                        DomainMembership.builder()
                                .domain(DOMAIN_NON_EMPTY)
                                .iamRoleName(DOMAIN_IAM_ROLE_NAME_NON_EMPTY)
                                .build()
                ))
                .build();
        final ResourceModel model = Translator.translateDbInstanceFromSdk(instance);
        assertThat(model.getDomain()).isEqualTo(DOMAIN_NON_EMPTY);
        assertThat(model.getDomainIAMRoleName()).isEqualTo(DOMAIN_IAM_ROLE_NAME_NON_EMPTY);
    }

    @Test
    public void test_translateDbInstanceFromSdkBuilder_noOptionGroupNameForEmptyOptionGroupMembership() {
        final DBInstance instance = DBInstance.builder()
                .build();
        final ResourceModel model = Translator.translateDbInstanceFromSdk(instance);
        assertThat(model.getOptionGroupName()).isNull();
    }

    @Test
    public void test_translateDbInstanceFromSdkBuilder_setOptionGroupFromOptionGroupMembership() {
        final String optionGroupName = TestUtils.randomString(32, TestUtils.ALPHA);
        final DBInstance instance = DBInstance.builder()
                .optionGroupMemberships(
                        OptionGroupMembership.builder().optionGroupName(optionGroupName).build()
                )
                .build();
        final ResourceModel model = Translator.translateDbInstanceFromSdk(instance);
        assertThat(model.getOptionGroupName()).isEqualTo(optionGroupName);
    }

    @Test
    public void test_modifyAfterCreate_shouldSetStorageParameters() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .storageType("gp3")
                .storageThroughput(100)
                .iops(200)
                .engine("sqlserver-ee")
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceAfterCreateRequest(model);
        assertThat(request.iops()).isEqualTo(200);
        assertThat(request.storageThroughput()).isEqualTo(100);
        assertThat(request.storageType()).isEqualTo("gp3");
    }

    @Test
    public void test_modifyAfterCreate_shouldSetGP3ParametersWhenPresent() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .storageType("gp3")
                .storageThroughput(100)
                .iops(200)
                .engine("sqlserver-ee")
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceAfterCreateRequest(model);
        assertThat(request.iops()).isEqualTo(200);
        assertThat(request.storageThroughput()).isEqualTo(100);
        assertThat(request.storageType()).isEqualTo("gp3");
    }

    @Test
    public void test_translateMasterUserSecret_sdkSecretEmpty() {
        assertThat(Translator.translateMasterUserSecret(null))
                .isEqualTo(MasterUserSecret.builder().build());
    }

    @Test
    public void test_translateMasterUserSecret_sdkSecretSet() {
        final software.amazon.rds.dbinstance.MasterUserSecret secret = Translator.translateMasterUserSecret(
                software.amazon.awssdk.services.rds.model.MasterUserSecret.builder()
                        .secretArn("arn")
                        .kmsKeyId("kmsKeyId")
                        .build());

        assertThat(secret.getSecretArn()).isEqualTo("arn");
        assertThat(secret.getKmsKeyId()).isEqualTo("kmsKeyId");
    }

    @Test
    public void test_translateManageMasterUserPassword_fromUnsetToUnset() {
        final ResourceModel prev = RESOURCE_MODEL_BLDR()
                .masterUserPassword("password")
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .masterUserPassword("password")
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isNull();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
        assertThat(request.masterUserPassword()).isNull();
    }

    @Test
    public void test_translateManageMasterUserPassword_fromSetToUnset() {
        final ResourceModel prev = RESOURCE_MODEL_BLDR()
                .manageMasterUserPassword(true)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("key").build())
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isFalse();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
    }

    @Test
    public void test_translateManageMasterUserPassword_explicitUnset() {
        final ResourceModel prev = RESOURCE_MODEL_BLDR()
                .manageMasterUserPassword(true)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("key1").build())
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .manageMasterUserPassword(false)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("key1").build())
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isFalse();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
    }

    @Test
    public void test_translateManageMasterUserPassword_fromUnsetToSet_withDefaultKey() {
        final ResourceModel prev = RESOURCE_MODEL_BLDR()
                .masterUserPassword("password")
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .manageMasterUserPassword(true)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId(null).build())
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isTrue();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
    }

    @Test
    public void test_translateManageMasterUserPassword_fromUnsetToSet_emptyMasterUserSecret() {
        final ResourceModel prev = RESOURCE_MODEL_BLDR()
                .masterUserPassword("password")
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .manageMasterUserPassword(true)
                .masterUserSecret(null)
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isTrue();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
    }

    @Test
    public void test_translateManageMasterUserPassword_fromExplicitFalseToExplicitFalse() {
        final ResourceModel prev = RESOURCE_MODEL_BLDR()
                .manageMasterUserPassword(false)
                .masterUserPassword("password")
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("key").build())
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .manageMasterUserPassword(false)
                .masterUserPassword("password")
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("key").build())

                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isNull();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
    }

    @Test
    public void test_translateManageMasterUserPassword_fromUnsetToSet_withSpecificKey() {
        final ResourceModel prev = RESOURCE_MODEL_BLDR()
                .masterUserPassword("password")
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .manageMasterUserPassword(true)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("myKey").build())
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isTrue();
        assertThat(request.masterUserSecretKmsKeyId()).isEqualTo("myKey");
    }

    @Test
    public void test_createDbInstanceRequest_AllowMajorVersionUpgrade() {
        ResourceModel model = ResourceModel.builder()
                .allowMajorVersionUpgrade(true)
                .build();
        final ModifyDbInstanceRequest modifyAfterCreateRequest = Translator.modifyDbInstanceAfterCreateRequest(
                model
        );
        assertThat(modifyAfterCreateRequest.allowMajorVersionUpgrade()).isTrue();
    }

    @Test
    public void test_createDbInstanceRequest_SetPerformanceInsightsKMSKeyIdIfEnabled() {
        final String kmsKeyId = "test-kms-key-id";
        final CreateDbInstanceRequest request = Translator.createDbInstanceRequest(
                ResourceModel.builder()
                        .enablePerformanceInsights(true)
                        .performanceInsightsKMSKeyId(kmsKeyId)
                        .build(),
                Tagging.TagSet.emptySet()
        );
        assertThat(request.enablePerformanceInsights()).isTrue();
        assertThat(request.performanceInsightsKMSKeyId()).isEqualTo(kmsKeyId);
    }

    @Test
    public void test_createDbInstanceRequest_DoNotSetPerformanceInsightsKMSKeyIdIfDisabled() {
        final String kmsKeyId = "test-kms-key-id";
        final CreateDbInstanceRequest request = Translator.createDbInstanceRequest(
                ResourceModel.builder()
                        .enablePerformanceInsights(false)
                        .performanceInsightsKMSKeyId(kmsKeyId)
                        .build(),
                Tagging.TagSet.emptySet()
        );
        assertThat(request.enablePerformanceInsights()).isNull();
        assertThat(request.performanceInsightsKMSKeyId()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequest_PerformanceInsightsEnabled() {
        final String kmsKeyId = "test-kms-key-id";
        final ResourceModel previousModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsKMSKeyId(kmsKeyId)
                .build();
        final ResourceModel desiredModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsKMSKeyId(kmsKeyId)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.enablePerformanceInsights()).isNull();
        assertThat(request.performanceInsightsKMSKeyId()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequest_PerformanceInsightsToggleDisabledToEnabled() {
        final String kmsKeyId = "test-kms-key-id";
        final ResourceModel previousModel = ResourceModel.builder()
                .enablePerformanceInsights(false)
                .performanceInsightsKMSKeyId(kmsKeyId)
                .build();
        final ResourceModel desiredModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsKMSKeyId(kmsKeyId)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.enablePerformanceInsights()).isTrue();
        assertThat(request.performanceInsightsKMSKeyId()).isEqualTo(kmsKeyId);
    }

    @Test
    public void test_modifyDbInstanceRequest_PerformanceInsightsToggleEnabledToDisabled() {
        final String kmsKeyId = "test-kms-key-id";
        final ResourceModel previousModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsKMSKeyId(kmsKeyId)
                .build();
        final ResourceModel desiredModel = ResourceModel.builder()
                .enablePerformanceInsights(false)
                .performanceInsightsKMSKeyId(kmsKeyId)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.enablePerformanceInsights()).isFalse();
        assertThat(request.performanceInsightsKMSKeyId()).isNull();
    }

    @Test
    public void test_modifyDbInstanceRequest_PerformanceInsightsChangeRetentionPeriod() {
        final int NEW_RETENTION_PERIOD = 31;
        final ResourceModel previousModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsRetentionPeriod(7)
                .build();
        final ResourceModel desiredModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsRetentionPeriod(NEW_RETENTION_PERIOD)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.enablePerformanceInsights()).isTrue();
        assertThat(request.performanceInsightsRetentionPeriod()).isEqualTo(NEW_RETENTION_PERIOD);
    }

    @Test
    public void test_modifyDbInstanceRequest_NoPerformanceInsightsChangeRetentionPeriod() {
        final int NEW_RETENTION_PERIOD = 31;
        final ResourceModel previousModel = ResourceModel.builder()
                .enablePerformanceInsights(false)
                .performanceInsightsRetentionPeriod(7)
                .build();
        final ResourceModel desiredModel = ResourceModel.builder()
                .enablePerformanceInsights(false)
                .performanceInsightsRetentionPeriod(NEW_RETENTION_PERIOD)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.enablePerformanceInsights()).isFalse();
        assertThat(request.performanceInsightsRetentionPeriod()).isEqualTo(NEW_RETENTION_PERIOD);
    }

    @Test
    public void test_modifyDbInstanceRequest_PerformanceInsightsEnabledChangeKMSKeyId() {
        final String kmsKeyId1 = "test-kms-key-id-1";
        final String kmsKeyId2 = "test-kms-key-id-2";
        final ResourceModel previousModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsKMSKeyId(kmsKeyId1)
                .build();
        final ResourceModel desiredModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsKMSKeyId(kmsKeyId2)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.enablePerformanceInsights()).isTrue();
        assertThat(request.performanceInsightsKMSKeyId()).isEqualTo(kmsKeyId2);
    }

    @Test
    public void test_modifyDbInstanceRequest_PerformanceInsightsDisabledChangeKMSKeyId() {
        final String kmsKeyId1 = "test-kms-key-id-1";
        final String kmsKeyId2 = "test-kms-key-id-2";
        final ResourceModel previousModel = ResourceModel.builder()
                .enablePerformanceInsights(false)
                .performanceInsightsKMSKeyId(kmsKeyId1)
                .build();
        final ResourceModel desiredModel = ResourceModel.builder()
                .enablePerformanceInsights(false)
                .performanceInsightsKMSKeyId(kmsKeyId2)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.enablePerformanceInsights()).isFalse();
        assertThat(request.performanceInsightsKMSKeyId()).isEqualTo(kmsKeyId2);
    }

    @Test
    public void test_modifyDbInstanceRequest_PerformanceInsightsToggleEnabledToDisabledChangeKMSKeyId() {
        final String kmsKeyId1 = "test-kms-key-id-1";
        final String kmsKeyId2 = "test-kms-key-id-2";
        final ResourceModel previousModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsKMSKeyId(kmsKeyId1)
                .build();
        final ResourceModel desiredModel = ResourceModel.builder()
                .enablePerformanceInsights(false)
                .performanceInsightsKMSKeyId(kmsKeyId2)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.enablePerformanceInsights()).isFalse();
        assertThat(request.performanceInsightsKMSKeyId()).isEqualTo(kmsKeyId2);
    }

    @Test
    public void test_modifyDbInstanceRequest_PerformanceInsightsToggleDisabledToEnabledChangeKMSKeyId() {
        final String kmsKeyId1 = "test-kms-key-id-1";
        final String kmsKeyId2 = "test-kms-key-id-2";
        final ResourceModel previousModel = ResourceModel.builder()
                .enablePerformanceInsights(false)
                .performanceInsightsKMSKeyId(kmsKeyId1)
                .build();
        final ResourceModel desiredModel = ResourceModel.builder()
                .enablePerformanceInsights(true)
                .performanceInsightsKMSKeyId(kmsKeyId2)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previousModel, desiredModel, false);
        assertThat(request.enablePerformanceInsights()).isTrue();
        assertThat(request.performanceInsightsKMSKeyId()).isEqualTo(kmsKeyId2);
    }

    @Test
    public void test_translateCertificateDetails_nullValue() {
        final CertificateDetails certificateDetails = Translator.translateCertificateDetailsFromSdk(null);
        assertThat(certificateDetails).isNull();
    }

    @Test
    public void test_translateCertificateDetails_setValues() {
        final Instant validTill = Instant.parse("2023-01-09T15:55:37.123Z");

        final CertificateDetails certificateDetails = Translator.translateCertificateDetailsFromSdk(
                software.amazon.awssdk.services.rds.model.CertificateDetails.builder()
                        .caIdentifier("identifier")
                        .validTill(validTill)
                        .build());

        assertThat(certificateDetails).isNotNull();
        assertThat(certificateDetails.getCAIdentifier()).isEqualTo("identifier");
        assertThat(certificateDetails.getValidTill()).isEqualTo("2023-01-09T15:55:37.123Z");
    }

    @Test
    public void translateDbInstanceFromSdk_port_getFromEndpoint() {
        final DBInstance dbInstance = DBInstance.builder()
                .endpoint(Endpoint.builder().port(123).build())
                .dbInstancePort(0)
                .build();

        final ResourceModel model = Translator.translateDbInstanceFromSdk(dbInstance);
        assertThat(model.getPort()).isEqualTo("123");
    }

    @Test
    public void modifyDbInstanceRequest_shouldIncludeAllocatedStorage_ifStorageTypeIsIo1_andIopsOnlyChanged() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR().storageType("io1").iops(1000).build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR().storageType("io1").iops(1200).build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, false);

        assertThat(request.iops()).isEqualTo(1200);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE);
    }

    @Test
    public void modifyDbInstanceRequest_shouldIncludeAllocatedStorageANdIops_ifStorageTypeIsIo1_andStorageTypeChangedToIo2() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR().storageType("io1").iops(1000).allocatedStorage("100").build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR().storageType("io2").iops(1000).allocatedStorage("100").build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, false);

        assertThat(request.iops()).isEqualTo(1000);
        assertThat(request.allocatedStorage()).isEqualTo(100);
    }

    @Test
    public void modifyDbInstanceRequest_shouldIncludeAllocatedStorage_ifStorageTypeIsImplicitIo1_andIopsOnlyChanged() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR().storageType(null).iops(1000).build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR().storageType(null).iops(1200).build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, false);

        assertThat(request.iops()).isEqualTo(1200);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE);
    }

    @Test
    public void modifyDbInstanceRequest_shouldIncludeAllocatedStorage_ifStorageTypeIsGp3_andIopsOnlyChanged() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR().storageType("gp3").iops(1000).build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR().storageType("gp3").iops(1200).build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, false);

        assertThat(request.iops()).isEqualTo(1200);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE);
    }

    @Test
    public void modifyDbInstanceRequest_shouldIncludeIops_ifStorageTypeIsGp3_andAllocatedStorageOnlyChanged() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR().storageType("gp3").allocatedStorage(ALLOCATED_STORAGE.toString()).build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR().storageType("gp3").allocatedStorage(ALLOCATED_STORAGE_INCR.toString()).build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, false);

        assertThat(request.iops()).isEqualTo(IOPS_DEFAULT);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_INCR);
    }

    @Test
    public void modifyDbInstanceRequest_shouldNotIncludeAllocatedStorage_ifStorageTypeIsGP2_andIopsOnlyChanged() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR().iops(1000).build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR().iops(1200).build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, false);

        assertThat(request.iops()).isEqualTo(1200);
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void modifyDbInstanceRequestV12_shouldIncludeAllocatedStorage_ifStorageTypeIsIo1_andIopsOnlyChanged() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR().storageType("io1").iops(1000).build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR().storageType("io1").iops(1200).build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previous, desired, false);

        assertThat(request.iops()).isEqualTo(1200);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE);
    }

    @Test
    public void modifyDbInstanceRequestV12_shouldIncludeAllocatedStorage_ifStorageTypeIsImplicitIo1_andIopsOnlyChanged() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR().storageType(null).iops(1000).build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR().storageType(null).iops(1200).build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previous, desired, false);

        assertThat(request.iops()).isEqualTo(1200);
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE);
    }

    @Test
    public void modifyDbInstanceRequestV12_shouldNotIncludeAllocatedStorage_ifStorageTypeIsGP2_andIopsOnlyChanged() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR().iops(1000).build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR().iops(1200).build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequestV12(previous, desired, false);

        assertThat(request.iops()).isEqualTo(1200);
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void modifyDbInstanceRequest_shouldIncludeAllocatedStorageAndIopsOnRollback_ifStorageTypeIsProvisioned_storageDecr() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR()
                .storageType(STORAGE_TYPE_IO1)
                .iops(IOPS_INCR)
                .allocatedStorage(ALLOCATED_STORAGE_INCR.toString())
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .storageType(STORAGE_TYPE_IO1)
                .iops(IOPS_DECR)
                .allocatedStorage(ALLOCATED_STORAGE_DECR.toString())
                .build();

        final boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, isRollback);

        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_INCR);
        assertThat(request.iops()).isEqualTo(IOPS_DECR);
    }

    @Test
    public void modifyDbInstanceRequest_sholdIncludeAllocatedStorageAndIopsOnRollback_ifStorageTypeIsProvisioned_storageNoChange() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR()
                .storageType(STORAGE_TYPE_IO1)
                .iops(IOPS_INCR)
                .allocatedStorage(ALLOCATED_STORAGE_INCR.toString())
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .storageType(STORAGE_TYPE_IO1)
                .iops(IOPS_DECR)
                .allocatedStorage(ALLOCATED_STORAGE_INCR.toString())
                .build();

        final boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, isRollback);

        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_INCR);
        assertThat(request.iops()).isEqualTo(IOPS_DECR);
    }

    @Test
    public void modifyDbInstanceRequest_shouldIncludeAllocatedStorageAndIopsOnRollback_ifStorageTypeIsProvisioned_storageUnset() {
        final ResourceModel previous = RESOURCE_MODEL_BLDR()
                .storageType(STORAGE_TYPE_IO1)
                .iops(IOPS_INCR)
                .allocatedStorage(ALLOCATED_STORAGE_INCR.toString())
                .build();
        final ResourceModel desired = RESOURCE_MODEL_BLDR()
                .storageType(STORAGE_TYPE_IO1)
                .iops(IOPS_DECR)
                .allocatedStorage(null)
                .build();

        final boolean isRollback = true;
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, isRollback);

        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_INCR);
        assertThat(request.iops()).isEqualTo(IOPS_DECR);
    }

    @Test
    public void test_modifyAfterCreate_shouldSetManageMasterUserPasswordFields() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .dBSnapshotIdentifier("snapshot")
                .manageMasterUserPassword(true)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("kms-key").build())
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceAfterCreateRequest(model);
        assertThat(request.manageMasterUserPassword()).isTrue();
        assertThat(request.masterUserSecretKmsKeyId()).isEqualTo("kms-key");
    }

    @Test
    public void test_modifyAfterCreate_shouldSetEnablePerformanceInsights() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .enablePerformanceInsights(true)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceAfterCreateRequest(model);
        assertThat(request.enablePerformanceInsights()).isTrue();
    }

    @Test
    public void test_modifyAfterCreate_shouldSetEnablePerformanceInsightsRetentionPeriod() {
        final int PI_RETENTION_PERIOD = 31;
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .enablePerformanceInsights(true)
                .performanceInsightsRetentionPeriod(PI_RETENTION_PERIOD)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceAfterCreateRequest(model);
        assertThat(request.enablePerformanceInsights()).isTrue();
        assertThat(request.performanceInsightsRetentionPeriod()).isEqualTo(PI_RETENTION_PERIOD);
    }

    @Test
    public void test_modifyAfterCreate_shouldSetEnhancedMonitoring() {
        final String monitoringRoleArn = "monitoring-role-arn";
        final int monitoringInterval = 42;
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .monitoringRoleArn(monitoringRoleArn)
                .monitoringInterval(monitoringInterval)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceAfterCreateRequest(model);

        assertThat(request.monitoringRoleArn()).isEqualTo(monitoringRoleArn);
        assertThat(request.monitoringInterval()).isEqualTo(monitoringInterval);
    }

    @Test
    public void test_modifyAfterCreate_shouldSetPerformanceInsightsKMSKeyIdIfPIEnabled() {
        final String kmsKeyId = "kms-key-id";
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .enablePerformanceInsights(true)
                .performanceInsightsKMSKeyId(kmsKeyId)
                .build();
        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceAfterCreateRequest(model);

        assertThat(request.performanceInsightsKMSKeyId()).isEqualTo(kmsKeyId);
    }

    @Test
    public void test_restoreDbInstanceFromSnapshot_shouldKeepCopyTagsToSnapshotEmptyIfUnset() {
        final ResourceModel model = ResourceModel.builder()
                .build();
        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequest(
                model,
                Tagging.TagSet.emptySet()
        );
        assertThat(request.copyTagsToSnapshot()).isNull();
    }

    @Test
    public void test_restoreDbInstanceFromSnapshot_shouldSetCopyTagsToSnapshot() {
        final ResourceModel model = ResourceModel.builder()
                .copyTagsToSnapshot(true)
                .build();
        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequest(
                model,
                Tagging.TagSet.emptySet()
        );
        assertThat(request.copyTagsToSnapshot()).isTrue();
    }

    @Test
    public void test_modifyDBInstance_dedicatedLogVolumeSplit() {
        final ResourceModel previous = ResourceModel.builder()
                .dedicatedLogVolume(false)
                .build();
        final ResourceModel desired = ResourceModel.builder()
                .dedicatedLogVolume(true)
                .build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(previous, desired, false);
        assertThat(request.dedicatedLogVolume()).isTrue();
    }

    // Stub methods to satisfy the interface. This is a 1-time thing.

    @Override
    protected BaseHandlerStd getHandler() {
        return null;
    }

    @Override
    protected AmazonWebServicesClientProxy getProxy() {
        return null;
    }

    @Override
    protected ProxyClient<RdsClient> getRdsProxy() {
        return null;
    }

    @Override
    protected ProxyClient<Ec2Client> getEc2Proxy() {
        return null;
    }

    @Override
    public HandlerName getHandlerName() {
        return null;
    }
}
