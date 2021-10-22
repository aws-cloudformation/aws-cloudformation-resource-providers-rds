package software.amazon.rds.common.handler;

import java.util.Arrays;
import java.util.Collections;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;

public final class Commons {

    private Commons() {
    }

    public static final ErrorRuleSet DEFAULT_ERROR_RULE_SET = ErrorRuleSet.builder()
            .withErrorCodes(Collections.singletonList(
                    ErrorCode.ClientUnavailable
            ), ErrorStatus.failWith(HandlerErrorCode.ServiceInternalError))
            .withErrorCodes(Arrays.asList(
                    ErrorCode.AccessDeniedException,
                    ErrorCode.NotAuthorized
            ), ErrorStatus.failWith(HandlerErrorCode.AccessDenied))
            .withErrorCodes(Collections.singletonList(
                    ErrorCode.ThrottlingException
            ), ErrorStatus.failWith(HandlerErrorCode.Throttling))
            .withErrorCodes(Arrays.asList(
                    ErrorCode.InvalidParameterCombination,
                    ErrorCode.InvalidParameterValue,
                    ErrorCode.MissingParameter
            ), ErrorStatus.failWith(HandlerErrorCode.InvalidRequest))
            .withErrorClasses(Collections.singletonList(
                    SdkClientException.class
            ), ErrorStatus.failWith(HandlerErrorCode.ServiceInternalError))
            .build();
}
