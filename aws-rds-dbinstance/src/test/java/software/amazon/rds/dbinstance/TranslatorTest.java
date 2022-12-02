package software.amazon.rds.dbinstance;

import static org.assertj.core.api.Assertions.assertThat;

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
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceFromDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbInstanceToPointInTimeRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.HandlerName;

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
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_INCR);
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
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE_INCR);
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
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE); // should stay unchanged
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
        assertThat(request.allocatedStorage()).isEqualTo(ALLOCATED_STORAGE); // should stay unchanged
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
        assertThat(request.iops()).isEqualTo(IOPS_INCR);
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
        assertThat(request.iops()).isEqualTo(IOPS_INCR);
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
        assertThat(request.iops()).isEqualTo(IOPS_DEFAULT);
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
        assertThat(request.iops()).isEqualTo(IOPS_DEFAULT);
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
    public void test_createReadReplicaRequest_parameterGroupNotSet() {
        final ResourceModel model = RESOURCE_MODEL_BLDR().build();

        final CreateDbInstanceReadReplicaRequest request = Translator.createDbInstanceReadReplicaRequest(model, Tagging.TagSet.builder().build());
        Assertions.assertNull(request.dbParameterGroupName());
    }

    @Test
    public void test_createReadReplicaRequest_blankSourceRegionIsNotSet() {
        final ResourceModel model = ResourceModel.builder()
                .sourceRegion("")
                .build();
        final CreateDbInstanceReadReplicaRequest request = Translator.createDbInstanceReadReplicaRequest(model, Tagging.TagSet.builder().build());
        Assertions.assertNull(request.sourceRegion());
    }

    @Test
    public void test_createReadReplicaRequest_nonBlankSourceRegionIsSet() {
        final String sourceRegion = "source-region";
        final ResourceModel model = ResourceModel.builder()
                .sourceRegion(sourceRegion)
                .build();
        final CreateDbInstanceReadReplicaRequest request = Translator.createDbInstanceReadReplicaRequest(model, Tagging.TagSet.builder().build());
        Assertions.assertEquals(sourceRegion, request.sourceRegion());
    }

    @Test
    public void test_modifyReadReplicaRequest_parameterGroupNotSet() {
        final ResourceModel model = RESOURCE_MODEL_BLDR().build();

        final ModifyDbInstanceRequest request = Translator.modifyDbInstanceRequest(ResourceModel.builder().build(), model, false);
        Assertions.assertEquals("default", request.dbParameterGroupName());
    }

    @Test
    public void test_restoreFromSnapshotRequest_storageTypeIO1() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .dBSnapshotIdentifier("snapshot")
                .storageType("io1")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isNull();
    }

    @Test
    public void test_restoreDbInstanceToPointInTimeRequest_storageTypeIO1() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .dBSnapshotIdentifier("snapshot")
                .storageType("io1")
                .build();

        final RestoreDbInstanceToPointInTimeRequest request = Translator.restoreDbInstanceToPointInTimeRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isNull();
    }

    @Test
    public void test_restoreFromSnapshotRequestV12_storageTypeIO1() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .dBSnapshotIdentifier("snapshot")
                .storageType("io1")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequestV12(model);
        assertThat(request.storageType()).isNull();
    }

    @Test
    public void test_restoreFromSnapshotRequest_storageTypeGp2() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isEqualTo("gp2");
    }

    @Test
    public void test_restoreDbInstanceToPointInTimeRequest_storageTypeGp2() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        final RestoreDbInstanceToPointInTimeRequest request = Translator.restoreDbInstanceToPointInTimeRequest(model, Tagging.TagSet.emptySet());
        assertThat(request.storageType()).isEqualTo("gp2");
    }

    @Test
    public void test_restoreFromSnapshotRequestV12_storageTypeGp2() {
        final ResourceModel model = RESOURCE_MODEL_BLDR()
                .dBSnapshotIdentifier("snapshot")
                .storageType("gp2")
                .build();

        final RestoreDbInstanceFromDbSnapshotRequest request = Translator.restoreDbInstanceFromSnapshotRequestV12(model);
        assertThat(request.storageType()).isEqualTo("gp2");
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
    public void test_canUpdateAllocatedStorage_nullArg() {
        assertThat(Translator.canUpdateAllocatedStorage(null, "42")).isTrue();
        assertThat(Translator.canUpdateAllocatedStorage("42", null)).isTrue();
    }

    @Test
    public void test_canUpdateAllocatedStorage_NumberFormatException() {
        assertThat(Translator.canUpdateAllocatedStorage("123", "invalid")).isTrue();
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
