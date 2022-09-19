package software.amazon.rds.test.common.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
class AbstractTestBaseTest {

    static class TestAbstractTestBase extends AbstractTestBase<Void, Void, Void> {

        @Override
        protected String getLogicalResourceIdentifier() {
            return "test-resource-identifier";
        }

        @Override
        protected void expectResourceSupply(Supplier<Void> supplier) {
        }

        @Override
        protected ProgressEvent<Void, Void> invokeHandleRequest(ResourceHandlerRequest<Void> request, Void context) {
            return null;
        }
    }

    @Test
    void newClientRequestToken() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        assertThat(testBase.newClientRequestToken()).isNotNull();
    }

    @Test
    void newStackId() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        assertThat(testBase.newStackId()).isNotNull();
    }

    @Test
    void expectInProgress_Success() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        final int pause = 10;
        final Consumer<ProgressEvent<Void, Void>> expectInProgress = testBase.expectInProgress(pause);
        final ProgressEvent<Void, Void> response = ProgressEvent.defaultInProgressHandler(null, pause, null);
        expectInProgress.accept(response); // should not throw
    }

    @Test
    void expectInProgress_Fail() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        final int pause = 10;
        final Consumer<ProgressEvent<Void, Void>> expectInProgress = testBase.expectInProgress(pause);
        final ProgressEvent<Void, Void> response = ProgressEvent.defaultSuccessHandler(null);
        Assertions.assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> expectInProgress.accept(response));
    }

    @Test
    void expectSuccess_Success() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        final Consumer<ProgressEvent<Void, Void>> expectSuccess = testBase.expectSuccess();
        final ProgressEvent<Void, Void> response = ProgressEvent.defaultSuccessHandler(null);
        expectSuccess.accept(response); // should not throw
    }

    @Test
    void expectSuccess_Fail() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        final Consumer<ProgressEvent<Void, Void>> expectSuccess = testBase.expectSuccess();
        final ProgressEvent<Void, Void> response = ProgressEvent.defaultInProgressHandler(null, 0, null);
        Assertions.assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> expectSuccess.accept(response));
    }

    @Test
    void expectFailed_Success() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        final Consumer<ProgressEvent<Void, Void>> expectFailed = testBase.expectFailed(HandlerErrorCode.InvalidRequest);
        final ProgressEvent<Void, Void> response = ProgressEvent.defaultFailureHandler(new Exception("test exception"), HandlerErrorCode.InvalidRequest);
        expectFailed.accept(response); // should not throw
    }

    @Test
    void expectFailed_Fail() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        final Consumer<ProgressEvent<Void, Void>> expectFailed = testBase.expectFailed(HandlerErrorCode.InvalidRequest);
        final ProgressEvent<Void, Void> response = ProgressEvent.defaultSuccessHandler(null);
        Assertions.assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> expectFailed.accept(response));
    }

    @Mock
    private ResourceHandlerRequest.ResourceHandlerRequestBuilder<Void> builder;

    @Test
    void test_handleRequest_base_ExpectResourceStateInvocation() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        Mockito.when(builder.desiredResourceState(ArgumentMatchers.any())).thenReturn(null);
        Mockito.when(builder.previousResourceState(ArgumentMatchers.any())).thenReturn(null);

        testBase.test_handleRequest_base(
                null,
                builder,
                () -> null,
                () -> null,
                () -> null,
                (progress) -> {
                }
        );

        Mockito.verify(builder, Mockito.times(1)).desiredResourceState(ArgumentMatchers.any());
        Mockito.verify(builder, Mockito.times(1)).previousResourceState(ArgumentMatchers.any());
    }

    @Test
    void test_handleRequest_base_ExpectNoPreviousStateInvocation() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        Mockito.when(builder.desiredResourceState(ArgumentMatchers.any())).thenReturn(null);

        testBase.test_handleRequest_base(
                null,
                builder,
                () -> null,
                null,
                () -> null,
                (progress) -> {
                }
        );

        Mockito.verify(builder, Mockito.times(1)).desiredResourceState(ArgumentMatchers.any());
    }

    @Test
    void test_handleRequest_base_NoSupplyExpect() {
        final TestAbstractTestBase testBase = new TestAbstractTestBase();
        Mockito.when(builder.desiredResourceState(ArgumentMatchers.any())).thenReturn(null);

        testBase.test_handleRequest_base(
                null,
                builder,
                null,
                null,
                () -> null,
                (progress) -> {
                }
        );

        Mockito.verify(builder, Mockito.times(1)).desiredResourceState(ArgumentMatchers.any());
    }
}
