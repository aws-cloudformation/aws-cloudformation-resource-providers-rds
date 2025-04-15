package software.amazon.rds.common.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.rds.common.logging.RequestLogger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

class IdempotencyHelperTest {
    private static final String MODEL = "blah";

    private RequestLogger mockRequestLogger = Mockito.mock(RequestLogger.class);
    private Context context = new Context();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void safeCreate_happy(boolean existenceCheckException) {
        AtomicBoolean checkedExistence = new AtomicBoolean(false);
        AtomicBoolean created = new AtomicBoolean(false);

        final var afterExistenceCheck = IdempotencyHelper.safeCreate(
            model -> {
                checkedExistence.set(true);
                Assertions.assertThat(model).isSameAs(MODEL);
                if (existenceCheckException) {
                    throw new CfnNotFoundException(new RuntimeException());
                } else {
                    return null;
                }
            },
            null,
            "resourceTypeName",
            "resourceIdentifier",
            ProgressEvent.progress(MODEL, context),
            mockRequestLogger
        );

        Assertions.assertThat(checkedExistence).isTrue();
        Assertions.assertThat(afterExistenceCheck.isInProgressCallbackDelay()).isTrue();
        Assertions.assertThat(afterExistenceCheck.getCallbackContext().getPreExistenceCheckDone()).isTrue();

        final var afterCreate = IdempotencyHelper.safeCreate(
            null,
            p -> {
                created.set(true);
                return ProgressEvent.defaultInProgressHandler(p.getCallbackContext(), 0, p.getResourceModel());
            },
            "resourceTypeName",
            "resourceIdentifier",
            ProgressEvent.progress(MODEL, context),
            mockRequestLogger
        );

        Assertions.assertThat(created).isTrue();
        Assertions.assertThat(afterCreate.canContinueProgress()).isTrue();
        Assertions.assertThat(afterCreate.getResourceModel()).isSameAs(MODEL);
        Assertions.assertThat(afterCreate.getCallbackContext()).isSameAs(context);
    }

    @Test
    void safeCreate_pre_already_exists() {
        Assertions.assertThatThrownBy(() -> {
            IdempotencyHelper.safeCreate(
                Function.identity(),
                null,
                "resourceTypeName",
                "resourceIdentifier",
                ProgressEvent.progress(MODEL, context),
                mockRequestLogger
            );
        }).isInstanceOf(CfnAlreadyExistsException.class);
    }

    @Test
    void safeCreate_failed_create() {
        context.setPreExistenceCheckDone(true);

        final var afterCreate = IdempotencyHelper.safeCreate(
            null,
            p -> ProgressEvent.defaultFailureHandler(new RuntimeException(), HandlerErrorCode.InternalFailure),
            "resourceTypeName",
            "resourceIdentifier",
            ProgressEvent.progress(MODEL, context),
            mockRequestLogger
        );

        Assertions.assertThat(afterCreate.isFailed()).isTrue();
        Assertions.assertThat(afterCreate.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
    }

    @Test
    void safeCreate_create_already_exists_after_pre_existence_check() {
        context.setPreExistenceCheckDone(true);

        final var afterCreate = IdempotencyHelper.safeCreate(
            null,
            p -> ProgressEvent.defaultFailureHandler(new RuntimeException(), HandlerErrorCode.AlreadyExists),
            "resourceTypeName",
            "resourceIdentifier",
            ProgressEvent.progress(MODEL, context),
            mockRequestLogger
        );

        Assertions.assertThat(afterCreate.canContinueProgress()).isTrue();
        Assertions.assertThat(afterCreate.getResourceModel()).isSameAs(MODEL);
        Assertions.assertThat(afterCreate.getCallbackContext()).isSameAs(context);
    }

    @lombok.Data
    private static class Context implements IdempotencyHelper.PreExistenceContext {
        private Boolean preExistenceCheckDone;
    }
}
