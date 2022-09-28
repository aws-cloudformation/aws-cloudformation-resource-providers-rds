package software.amazon.rds.dbcluster;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Override
    protected BaseHandlerStd getHandler() { return null; }

    @Override
    protected AmazonWebServicesClientProxy getProxy() { return null; }

    @Override
    protected ProxyClient<RdsClient> getRdsProxy() { return null; }

    @Override
    protected ProxyClient<Ec2Client> getEc2Proxy() { return null; }

    @Override
    public HandlerName getHandlerName() {
        return null;
    }
}
