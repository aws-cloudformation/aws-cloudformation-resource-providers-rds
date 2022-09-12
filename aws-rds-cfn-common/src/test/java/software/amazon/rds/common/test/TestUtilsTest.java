package software.amazon.rds.common.test;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtilsTest {
    @Test
    public void test_randomString() {
        final int length = 16;
        final String alphabet = "abc";

        final String randStr = TestUtils.randomString(length, alphabet);
        assertThat(randStr.length()).isEqualTo(length);

        final String resultAlphabet = randStr.chars()
                .distinct()
                .sorted()
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        assertThat(alphabet.contains(resultAlphabet)).isTrue();
    }
}
