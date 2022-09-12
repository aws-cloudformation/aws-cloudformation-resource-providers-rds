package software.amazon.rds.common.test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.rds.common.error.ErrorCode;

import java.security.SecureRandom;
import java.util.UUID;

public final class TestUtils {
    public final static SecureRandom random = new SecureRandom();
    final public static String ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    final public static String ALPHANUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    public static String newClientRequestToken() {
        return UUID.randomUUID().toString();
    }

    public static String newStackId() {
        return UUID.randomUUID().toString();
    }

    public static String randomString(final int length, final String alphabet) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return builder.toString();
    }

    public static AwsServiceException newAwsServiceException(final ErrorCode errorCode) {
        return AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build())
                .build();
    }
}
