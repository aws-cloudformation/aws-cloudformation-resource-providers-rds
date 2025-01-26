package software.amazon.rds.dbinstance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.PendingModifiedValues;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.dbinstance.status.DBInstanceStatus;

import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DBInstancePredicatesTest {
    @Mock
    RequestLogger requestLogger = Mockito.mock(RequestLogger.class);

    @ParameterizedTest
    @MethodSource("stabilizeTestCases")
    public void test_isDBInstanceStabilizedAfterMutate(Boolean applyImmediate, Boolean isStabilized){
        DBInstance dbInstance = DBInstance.builder()
            .dbInstanceStatus(DBInstanceStatus.Available.toString())
            .pendingModifiedValues(PendingModifiedValues.builder()
                .backupRetentionPeriod(5)
                .build())
            .build();

        ResourceModel model = ResourceModel.builder()
            .applyImmediately(applyImmediate)
            .build();

        boolean actual = DBInstancePredicates.isDBInstanceStabilizedAfterMutate(dbInstance, model, null, requestLogger);

        assertThat(actual).isEqualTo(isStabilized);
    }

    private static Stream<Arguments> stabilizeTestCases() {
        return Stream.of(
            Arguments.of(null, Boolean.FALSE),
            Arguments.of(Boolean.TRUE, Boolean.FALSE),
            Arguments.of(Boolean.FALSE, Boolean.TRUE)
        );
    }
}
