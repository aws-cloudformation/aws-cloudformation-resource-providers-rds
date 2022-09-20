package software.amazon.rds.test.common.core;

import java.security.SecureRandom;

import org.mockito.internal.util.MockUtil;
import org.mockito.internal.verification.VerificationDataImpl;
import org.mockito.internal.verification.api.VerificationData;

public final class TestUtils {
    final private static SecureRandom random = new SecureRandom();

    private TestUtils() {}

    final public static String ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    final public static String ALPHANUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    public static String randomString(final int length, final String alphabet) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return builder.toString();
    }

    public static VerificationData getVerificationData(final Object mock) {
        return new VerificationDataImpl(MockUtil.getInvocationContainer(mock), null);
    }
}
