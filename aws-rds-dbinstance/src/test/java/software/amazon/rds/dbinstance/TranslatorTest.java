package software.amazon.rds.dbinstance;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbInstanceRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.Tagging;

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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
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
        final ModifyDbInstanceRequest request = TranslatorV19.modifyDbInstanceRequest(previousModel, desiredModel, isRollback);
        assertThat(request.processorFeatures()).hasSameElementsAs(TranslatorV19.translateProcessorFeaturesToSdk(PROCESSOR_FEATURES_ALTER));
    }

    @Test
    public void test_createDBInstanceRequest_stableHashCode() {
        final Tagging.TagSet tagSet1 = Tagging.TagSet.builder()
                .systemTags(TranslatorV19.translateTagsToSdk(TAG_LIST))
                .resourceTags(TranslatorV19.translateTagsToSdk(TAG_LIST))
                .stackTags(TranslatorV19.translateTagsToSdk(TAG_LIST))
                .build();
        final Tagging.TagSet tagSet2 = Tagging.TagSet.builder()
                .systemTags(TranslatorV19.translateTagsToSdk(TAG_LIST))
                .resourceTags(TranslatorV19.translateTagsToSdk(TAG_LIST))
                .stackTags(TranslatorV19.translateTagsToSdk(TAG_LIST))
                .build();
        final CreateDbInstanceRequest request1 = TranslatorV19.createDbInstanceRequest(RESOURCE_MODEL_BLDR().build(), tagSet1);
        final CreateDbInstanceRequest request2 = TranslatorV19.createDbInstanceRequest(RESOURCE_MODEL_BLDR().build(), tagSet2);

        Assertions.assertEquals(request1.hashCode(), request2.hashCode());
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
}
