package software.amazon.rds.test.common.core;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestUtilsTest {

    @Test
    public void test_randomString() {
        final int length = 16;
        final String alphabet = "abc";

        final String randStr = TestUtils.randomString(length, alphabet);
        Assertions.assertThat(randStr.length()).isEqualTo(length);

        final String resultAlphabet = randStr.chars()
                .distinct()
                .sorted()
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        Assertions.assertThat(alphabet.contains(resultAlphabet)).isTrue();
    }
}
