package software.amazon.rds.dbcluster.validators;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.dbcluster.ResourceModel;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClusterScalabilityTypeValidatorTest {

    private ResourceModel model;

    @BeforeEach
    void setUp() {
        model = new ResourceModel();
    }

    @Test
    void validateRequest_withPointInTimeRestoreAndScalabilityType_throwsException() {
        model.setSourceDBClusterIdentifier("source-cluster-id");
        model.setClusterScalabilityType("STANDARD");

        assertThatThrownBy(() -> ClusterScalabilityTypeValidator.validateRequest(model))
            .isInstanceOf(RequestValidationException.class);
        }

    @Test
    void validateRequest_withPointInTimeRestoreAndNoScalabilityType_doesNotThrow() {
        model.setSourceDBClusterIdentifier("source-cluster-id");
        model.setClusterScalabilityType(null);

        assertThatNoException().isThrownBy(() -> ClusterScalabilityTypeValidator.validateRequest(model));
    }

    @Test
    void validateRequest_withSnapshotRestoreAndScalabilityType_throwsException() {
        model.setSnapshotIdentifier("snapshot-id");
        model.setClusterScalabilityType("STANDARD");

        assertThatThrownBy(() -> ClusterScalabilityTypeValidator.validateRequest(model))
            .isInstanceOf(RequestValidationException.class);
    }

    @Test
    void validateRequest_withSnapshotRestoreAndNoScalabilityType_doesNotThrow() {
        model.setSnapshotIdentifier("snapshot-id");
        model.setClusterScalabilityType(null);

        assertThatNoException().isThrownBy(() -> ClusterScalabilityTypeValidator.validateRequest(model));
    }

    @Test
    void validateRequest_withNoRestoreAndScalabilityType_doesNotThrow() {
        model.setSourceDBClusterIdentifier(null);
        model.setSnapshotIdentifier(null);
        model.setClusterScalabilityType("STANDARD");

        assertThatNoException().isThrownBy(() -> ClusterScalabilityTypeValidator.validateRequest(model));
    }

    @Test
    void validateRequest_withNoRestoreAndNoScalabilityType_doesNotThrow() {
        model.setSourceDBClusterIdentifier(null);
        model.setSnapshotIdentifier(null);
        model.setClusterScalabilityType(null);

        assertThatNoException().isThrownBy(() -> ClusterScalabilityTypeValidator.validateRequest(model));
    }
}
