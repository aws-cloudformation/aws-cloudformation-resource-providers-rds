package software.amazon.rds.dbcluster;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DomainMembership;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RestoreDbClusterToPointInTimeRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;

public class TranslatorTest extends AbstractHandlerTest {

    @Test
    public void modifyDbClusterRequest_omitPreferredMaintenanceWindowIfUnchanged() {
        final ResourceModel model = RESOURCE_MODEL.toBuilder().preferredMaintenanceWindow("old").build();
        final Boolean isRollback = false;

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(model, model, isRollback);
        assertThat(request.preferredMaintenanceWindow()).isNull();
    }

    @Test
    public void modifyDbClusterRequest_setPreferredMaintenanceWindow() {
        final ResourceModel previousModel = RESOURCE_MODEL.toBuilder().preferredMaintenanceWindow("old").build();

        final ResourceModel desiredModel = RESOURCE_MODEL.toBuilder().preferredMaintenanceWindow("new").build();
        final Boolean isRollback = false;

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(previousModel, desiredModel, isRollback);
        assertThat(request.preferredMaintenanceWindow()).isEqualTo("new");
    }

    @Test
    public void modifyDbClusterRequest_omitPreferredBackupWindowIfUnchanged() {
        final ResourceModel model = RESOURCE_MODEL.toBuilder().preferredBackupWindow("old").build();
        final Boolean isRollback = false;

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(model, model, isRollback);
        assertThat(request.preferredBackupWindow()).isNull();
    }

    @Test
    public void modifyDbClusterRequest_setPreferredBackupWindowWindow() {
        final ResourceModel previousModel = RESOURCE_MODEL.toBuilder().preferredBackupWindow("old").build();
        final ResourceModel desiredModel = RESOURCE_MODEL.toBuilder().preferredBackupWindow("new").build();
        final Boolean isRollback = false;

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(previousModel, desiredModel, isRollback);
        assertThat(request.preferredBackupWindow()).isEqualTo("new");
    }

    @Test
    public void modifyDbClusterRequest_omitEnableIAMDatabaseAuthenticationIfUnchanged() {
        final ResourceModel model = RESOURCE_MODEL.toBuilder().enableIAMDatabaseAuthentication(true).build();
        final Boolean isRollback = false;

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(model, model, isRollback);
        assertThat(request.enableIAMDatabaseAuthentication()).isNull();
    }

    @Test
    public void modifyDbClusterRequest_setEnableIAMDatabaseAuthentication() {
        final ResourceModel previousModel = RESOURCE_MODEL.toBuilder().enableIAMDatabaseAuthentication(false).build();
        final ResourceModel desiredModel = RESOURCE_MODEL.toBuilder().enableIAMDatabaseAuthentication(true).build();
        final Boolean isRollback = false;

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(previousModel, desiredModel, isRollback);
        assertThat(request.enableIAMDatabaseAuthentication()).isEqualTo(Boolean.TRUE);
    }

    @Test
    public void ModifyDbClusterRequest_dbInstanceParameterGroupNameIsNotSetWhenEngineVersionIsNotUpgrading() {
        final ResourceModel previousModel = RESOURCE_MODEL.toBuilder().engineVersion("old-engine").dBInstanceParameterGroupName("old-pg").build();
        final ResourceModel desiredModel = RESOURCE_MODEL.toBuilder().engineVersion("old-engine").dBInstanceParameterGroupName("new-pg").build();

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(previousModel, desiredModel, false);
        assertThat(request.dbInstanceParameterGroupName()).isBlank();
    }

    @Test
    public void ModifyDbClusterRequest_dbInstanceParameterGroupNameIsSetWhenEngineVersionIsUpgrading() {
        final ResourceModel previousModel = RESOURCE_MODEL.toBuilder().engineVersion("old-engine").dBInstanceParameterGroupName("old-pg").build();
        final ResourceModel desiredModel = RESOURCE_MODEL.toBuilder().engineVersion("new-engine").dBInstanceParameterGroupName("new-pg").build();

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(previousModel, desiredModel, false);
        assertThat(request.dbInstanceParameterGroupName()).isEqualTo("new-pg");
    }

    @Test
    public void ModifyDbClusterRequest_dbInstanceParameterGroupNameIsNotSetDuringRollback() {
        final ResourceModel previousModel = RESOURCE_MODEL.toBuilder().engineVersion("old-engine").dBInstanceParameterGroupName("old-pg").build();
        final ResourceModel desiredModel = RESOURCE_MODEL.toBuilder().engineVersion("new-engine").dBInstanceParameterGroupName("new-pg").build();

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(previousModel, desiredModel, true);
        assertThat(request.dbInstanceParameterGroupName()).isBlank();
    }

    @Test
    public void test_translateDbClusterFromSdk_emptyDomainMembership() {
        final DBCluster cluster = DBCluster.builder()
                .domainMemberships((Collection<DomainMembership>) null)
                .build();
        final ResourceModel model = Translator.translateDbClusterFromSdk(cluster);
        assertThat(model.getDomain()).isNull();
        assertThat(model.getDomainIAMRoleName()).isNull();
    }

    @Test
    public void test_translateDbClusterFromSdk_withDomainMembership() {
        final DBCluster cluster = DBCluster.builder()
                .domainMemberships(ImmutableList.of(
                        DomainMembership.builder()
                                .domain(DOMAIN_NON_EMPTY)
                                .iamRoleName(DOMAIN_IAM_ROLE_NAME_NON_EMPTY)
                                .build()
                ))
                .build();
        final ResourceModel model = Translator.translateDbClusterFromSdk(cluster);
        assertThat(model.getDomain()).isEqualTo(DOMAIN_NON_EMPTY);
        assertThat(model.getDomainIAMRoleName()).isEqualTo(DOMAIN_IAM_ROLE_NAME_NON_EMPTY);
    }

    @Test
    public void translateManageMasterUserPassword_fromUnsetToUnset() {
        final ResourceModel prev = RESOURCE_MODEL.toBuilder()
                .masterUserPassword("password")
                .build();
        final ResourceModel desired = RESOURCE_MODEL.toBuilder()
                .masterUserPassword("password")
                .build();

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isNull();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
        assertThat(request.masterUserPassword()).isNull();
    }

    @Test
    public void translateManageMasterUserPassword_fromSetToUnset() {
        final ResourceModel prev = RESOURCE_MODEL.toBuilder()
                .manageMasterUserPassword(true)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("key").build())
                .build();
        final ResourceModel desired = RESOURCE_MODEL.toBuilder()
                .build();

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isFalse();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
    }

    @Test
    public void translateManageMasterUserPassword_explicitUnset() {
        final ResourceModel prev = RESOURCE_MODEL.toBuilder()
                .manageMasterUserPassword(true)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("key1").build())
                .build();
        final ResourceModel desired = RESOURCE_MODEL.toBuilder()
                .manageMasterUserPassword(false)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("key1").build())
                .build();

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isFalse();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
    }

    @Test
    public void translateManageMasterUserPassword_fromUnsetToSet_withDefaultKey() {
        final ResourceModel prev = RESOURCE_MODEL.toBuilder()
                .masterUserPassword("password")
                .build();
        final ResourceModel desired = RESOURCE_MODEL.toBuilder()
                .manageMasterUserPassword(true)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId(null).build())
                .build();

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isTrue();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
    }

    @Test
    public void translateManageMasterUserPassword_fromUnsetToSet_withSpecificKey() {
        final ResourceModel prev = RESOURCE_MODEL.toBuilder()
                .masterUserPassword("password")
                .build();
        final ResourceModel desired = RESOURCE_MODEL.toBuilder()
                .manageMasterUserPassword(true)
                .masterUserSecret(MasterUserSecret.builder().kmsKeyId("myKey").build())
                .build();

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(prev, desired, false);

        assertThat(request.manageMasterUserPassword()).isTrue();
        assertThat(request.masterUserSecretKmsKeyId()).isEqualTo("myKey");
    }

    @Test
    public void test_translateScalingConfigurationToSdk_null() {
        assertThat(Translator.translateScalingConfigurationToSdk(null)).isNull();
    }

    @Test
    public void test_translateScalingConfigurationToSdk_SetAttributes() {
        final Random random = new Random();
        final ScalingConfiguration config = ScalingConfiguration.builder()
                .autoPause(true)
                .minCapacity(random.nextInt())
                .maxCapacity(random.nextInt())
                .timeoutAction(TestUtils.randomString(16, TestUtils.ALPHA))
                .secondsBeforeTimeout(random.nextInt())
                .secondsUntilAutoPause(random.nextInt())
                .build();

        final software.amazon.awssdk.services.rds.model.ScalingConfiguration result = Translator.translateScalingConfigurationToSdk(config);

        assertThat(result.autoPause()).isEqualTo(config.getAutoPause());
        assertThat(result.minCapacity()).isEqualTo(config.getMinCapacity());
        assertThat(result.maxCapacity()).isEqualTo(config.getMaxCapacity());
        assertThat(result.secondsBeforeTimeout()).isEqualTo(config.getSecondsBeforeTimeout());
        assertThat(result.secondsUntilAutoPause()).isEqualTo(config.getSecondsUntilAutoPause());
    }

    @Test
    public void test_translateScalingConfigurationFromSdk_null() {
        assertThat(Translator.translateScalingConfigurationFromSdk(null)).isNull();
    }

    @Test
    public void test_translateScalingConfigurationFromSdk_SetAttributes() {
        final Random random = new Random();
        final software.amazon.awssdk.services.rds.model.ScalingConfigurationInfo config = software.amazon.awssdk.services.rds.model.ScalingConfigurationInfo.builder()
                .autoPause(true)
                .minCapacity(random.nextInt())
                .maxCapacity(random.nextInt())
                .timeoutAction(TestUtils.randomString(16, TestUtils.ALPHA))
                .secondsBeforeTimeout(random.nextInt())
                .secondsUntilAutoPause(random.nextInt())
                .build();

        final ScalingConfiguration result = Translator.translateScalingConfigurationFromSdk(config);

        assertThat(result.getAutoPause()).isEqualTo(config.autoPause());
        assertThat(result.getMinCapacity()).isEqualTo(config.minCapacity());
        assertThat(result.getMaxCapacity()).isEqualTo(config.maxCapacity());
        assertThat(result.getSecondsBeforeTimeout()).isEqualTo(config.secondsBeforeTimeout());
        assertThat(result.getSecondsUntilAutoPause()).isEqualTo(config.secondsUntilAutoPause());
    }

    @Test
    public void test_modifyAfterCreateWhenManageMasterUserPasswordIsTrueWithImplicitDefaults() {
        final ResourceModel desired = RESOURCE_MODEL.toBuilder()
                .manageMasterUserPassword(true)
                .build();

        final ModifyDbClusterRequest request = Translator.modifyDbClusterRequest(desired, desired, false);

        assertThat(request.manageMasterUserPassword()).isTrue();
        assertThat(request.masterUserSecretKmsKeyId()).isNull();
    }

    @Test
    public void translateMasterUserSecretFromSdkTranslatesFields() {
        final software.amazon.awssdk.services.rds.model.MasterUserSecret masterUserSecret = software.amazon.awssdk.services.rds.model.MasterUserSecret.builder()
                .secretArn("secret-arn")
                .kmsKeyId("key")
                .build();

        final MasterUserSecret translated = Translator.translateMasterUserSecretFromSdk(masterUserSecret);

        assertThat(translated.getSecretArn()).isEqualTo("secret-arn");
        assertThat(translated.getKmsKeyId()).isEqualTo("key");
    }

    @Test
    public void translateMasterUserSecretFromSdkTranslatesNullValue() {
        final MasterUserSecret translated = Translator.translateMasterUserSecretFromSdk(null);
        assertThat(translated).isNotNull();
        assertThat(translated.getSecretArn()).isNull();
        assertThat(translated.getKmsKeyId()).isNull();
    }

    @Test
    public void modifyDBClusterAfterCreateServerlessShouldNotSetUnsupportedParameters() {
        final ResourceModel model = ResourceModel.builder()
                .engineMode(EngineMode.Serverless.toString())
                .enableIAMDatabaseAuthentication(true)
                .preferredBackupWindow("backup")
                .backtrackWindow(100)
                .allocatedStorage(200)
                .build();
        final ModifyDbClusterRequest request = Translator.modifyDbClusterAfterCreateRequest(model);

        assertThat(request.enableIAMDatabaseAuthentication()).isNull();
        assertThat(request.preferredBackupWindow()).isNull();
        assertThat(request.backtrackWindow()).isNull();
        assertThat(request.allocatedStorage()).isNull();
    }

    @Test
    public void modifyDBClusterAfterCreateNonServerless() {
        final ResourceModel model = ResourceModel.builder()
                .engineMode(EngineMode.Provisioned.toString())
                .enableIAMDatabaseAuthentication(true)
                .preferredBackupWindow("backup")
                .backtrackWindow(100)
                .allocatedStorage(200)
                .build();
        final ModifyDbClusterRequest request = Translator.modifyDbClusterAfterCreateRequest(model);

        assertThat(request.enableIAMDatabaseAuthentication()).isEqualTo(true);
        assertThat(request.preferredBackupWindow()).isEqualTo("backup");
        assertThat(request.backtrackWindow()).isEqualTo(100);
        assertThat(request.allocatedStorage()).isEqualTo(200);
    }

    @Test
    public void restoreDBClusterToPointInTime() {
        final ResourceModel model = ResourceModel.builder()
                .engineMode(EngineMode.Provisioned.toString())
                .enableIAMDatabaseAuthentication(true)
                .useLatestRestorableTime(false)
                .restoreToTime("2019-03-07T23:45:00Z")
                .build();
        final RestoreDbClusterToPointInTimeRequest request = Translator.restoreDbClusterToPointInTimeRequest(model, Tagging.TagSet.emptySet());

        assertThat(request.useLatestRestorableTime()).isEqualTo(false);
        assertThat(request.restoreToTime()).isEqualTo("2019-03-07T23:45:00Z");
    }

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
