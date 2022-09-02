package software.amazon.rds.common.test;

import java.security.SecureRandom;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;

public abstract class AbstractTestBase<ResourceT, ModelT, ContextT> {

    final private static SecureRandom random = new SecureRandom();
    final public static String ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    final public static String ALPHANUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    protected abstract String getLogicalResourceIdentifier();

    protected abstract void expectResourceSupply(final Supplier<ResourceT> supplier);

    protected abstract ProgressEvent<ModelT, ContextT> invokeHandleRequest(ResourceHandlerRequest<ModelT> request, ContextT context);

    protected String newClientRequestToken() {
        return UUID.randomUUID().toString();
    }

    protected String newStackId() {
        return UUID.randomUUID().toString();
    }

    protected String randomString(final int length, final String alphabet) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return builder.toString();
    }

    protected Consumer<ProgressEvent<ModelT, ContextT>> expectInProgress(int pause) {
        return (response) -> {
            Assertions.assertThat(response).isNotNull();
            Assertions.assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
            Assertions.assertThat(response.getCallbackDelaySeconds()).isEqualTo(pause);
            Assertions.assertThat(response.getResourceModels()).isNull();
            Assertions.assertThat(response.getMessage()).isNull();
            Assertions.assertThat(response.getErrorCode()).isNull();
        };
    }

    protected Consumer<ProgressEvent<ModelT, ContextT>> expectSuccess() {
        return (response) -> {
            Assertions.assertThat(response).isNotNull();
            Assertions.assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
            Assertions.assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
            Assertions.assertThat(response.getMessage()).isNull();
            Assertions.assertThat(response.getErrorCode()).isNull();
        };
    }

    protected Consumer<ProgressEvent<ModelT, ContextT>> expectFailed(final HandlerErrorCode errorCode) {
        return (response) -> {
            Assertions.assertThat(response).isNotNull();
            Assertions.assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
            Assertions.assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
            Assertions.assertThat(response.getMessage()).isNotNull();
            Assertions.assertThat(response.getErrorCode()).isEqualTo(errorCode);
            Assertions.assertThat(response.getResourceModels()).isNull();
        };
    }

    protected ProgressEvent<ModelT, ContextT> test_handleRequest_base(
            final ContextT context,
            final Supplier<ResourceT> resourceSupplier,
            final Supplier<ModelT> desiredStateSupplier,
            final Consumer<ProgressEvent<ModelT, ContextT>> expect
    ) {
        return test_handleRequest_base(context, resourceSupplier, null, desiredStateSupplier, expect);
    }

    protected ProgressEvent<ModelT, ContextT> test_handleRequest_base(
            final ContextT context,
            final Supplier<ResourceT> resourceSupplier,
            final Supplier<ModelT> previousStateSupplier,
            final Supplier<ModelT> desiredStateSupplier,
            final Consumer<ProgressEvent<ModelT, ContextT>> expect
    ) {
        return test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ModelT>builder(),
                resourceSupplier,
                previousStateSupplier,
                desiredStateSupplier,
                expect
        );
    }

    protected ProgressEvent<ModelT, ContextT> test_handleRequest_base(
            final ContextT context,
            final ResourceHandlerRequest.ResourceHandlerRequestBuilder<ModelT> builder,
            final Supplier<ResourceT> resourceSupplier,
            final Supplier<ModelT> previousStateSupplier,
            final Supplier<ModelT> desiredStateSupplier,
            final Consumer<ProgressEvent<ModelT, ContextT>> expect
    ) {
        if (resourceSupplier != null) {
            expectResourceSupply(resourceSupplier);
        }

        builder.desiredResourceState(desiredStateSupplier.get());
        if (previousStateSupplier != null) {
            builder.previousResourceState(previousStateSupplier.get());
        }
        builder.logicalResourceIdentifier(getLogicalResourceIdentifier());
        builder.clientRequestToken(newClientRequestToken());
        builder.stackId(newStackId());

        final ProgressEvent<ModelT, ContextT> response = invokeHandleRequest(builder.build(), context);
        expect.accept(response);

        return response;
    }

    protected static AwsServiceException newAwsServiceException(final ErrorCode errorCode) {
        return AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build())
                .build();
    }

    @ExcludeFromJacocoGeneratedReport
    protected <RequestT extends AwsRequest, ResponseT extends AwsResponse> void test_handleRequest_error(
            final MethodCallExpectation<RequestT, ResponseT> expectation,
            final ContextT context,
            final Supplier<ModelT> desiredStateSupplier,
            final Object requestException,
            final HandlerErrorCode errorCode
    ) {
        test_handleRequest_error(
                expectation,
                context,
                null,
                desiredStateSupplier,
                requestException,
                errorCode
        );
    }

    @ExcludeFromJacocoGeneratedReport
    protected <RequestT extends AwsRequest, ResponseT extends AwsResponse> void test_handleRequest_error(
            final MethodCallExpectation<RequestT, ResponseT> expectation,
            final ContextT context,
            final Supplier<ModelT> previousStateSupplier,
            final Supplier<ModelT> desiredStateSupplier,
            final Object requestException,
            final HandlerErrorCode expectErrorCode
    ) {
        final Exception exception = requestException instanceof ErrorCode ? newAwsServiceException((ErrorCode) requestException) : (Exception) requestException;

        expectation.setup()
                .thenThrow(exception);

        test_handleRequest_base(
                context,
                null,
                previousStateSupplier,
                desiredStateSupplier,
                expectFailed(expectErrorCode)
        );

        expectation.verify();
    }
}
