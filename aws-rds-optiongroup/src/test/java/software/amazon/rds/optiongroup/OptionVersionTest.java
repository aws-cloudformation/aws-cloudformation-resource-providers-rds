package software.amazon.rds.optiongroup;

import org.junit.jupiter.api.Test;

public class OptionVersionTest {

    @Test
    public void compareTo_simple_comparison_major() {
        final OptionVersion v1 = new OptionVersion("1.2.3");
        final OptionVersion v2 = new OptionVersion("2.3.4");
        assert (v1.compareTo(v2) < 0);
    }

    @Test
    void compareTo_simple_comparison_minor() {
        final OptionVersion v1 = new OptionVersion("1.2.3");
        final OptionVersion v2 = new OptionVersion("1.3.3");
        assert (v1.compareTo(v2) < 0);
    }

    @Test
    void compareTo_simple_comparison_single_digit() {
        final OptionVersion v1 = new OptionVersion("1.2.3");
        final OptionVersion v2 = new OptionVersion("1");
        assert (v1.compareTo(v2) > 0);
    }

    @Test
    public void compareTo_shorter_version() {
        final OptionVersion v1 = new OptionVersion("1.2.3.4.v1");
        final OptionVersion v2 = new OptionVersion("1.2.3.4.v1.v2");
        assert (v1.compareTo(v2) < 0);
    }

    @Test
    public void compareTo_ignore_non_digital_part() {
        final OptionVersion v1 = new OptionVersion("5.1.2.v1");
        final OptionVersion v2 = new OptionVersion("5.1.2.v2");
        // 5.1.2.v1 equals to 5.1.2.v2
        assert (v1.compareTo(v2) == 0);
    }
}
