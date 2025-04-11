package software.amazon.rds.common.handler;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VpcTest {


    @Test
    void shouldSetDefaultVpcId_previousNonEmpty_desiredNull_returnsTrue() {
        List<String> previous = Arrays.asList("sg-123", "sg-456");
        List<String> desired = null;
        assertTrue(Vpc.shouldSetDefaultVpcId(previous, desired));
    }

    @Test
    void shouldSetDefaultVpcId_previousNonEmpty_desiredEmpty_returnsTrue() {
        List<String> previous = Arrays.asList("sg-123");
        List<String> desired = Collections.emptyList();
        assertTrue(Vpc.shouldSetDefaultVpcId(previous, desired));
    }

    @Test
    void shouldSetDefaultVpcId_previousNull_desiredNull_returnsFalse() {
        List<String> previous = null;
        List<String> desired = null;
        assertFalse(Vpc.shouldSetDefaultVpcId(previous, desired));
    }

    @Test
    void shouldSetDefaultVpcId_previousEmpty_desiredNull_returnsFalse() {
        List<String> previous = Collections.emptyList();
        List<String> desired = null;
        assertFalse(Vpc.shouldSetDefaultVpcId(previous, desired));
    }

    @Test
    void shouldSetDefaultVpcId_previousNull_desiredEmpty_returnsFalse() {
        List<String> previous = null;
        List<String> desired = Collections.emptyList();
        assertFalse(Vpc.shouldSetDefaultVpcId(previous, desired));
    }

    @Test
    void shouldSetDefaultVpcId_previousEmpty_desiredEmpty_returnsFalse() {
        List<String> previous = Collections.emptyList();
        List<String> desired = Collections.emptyList();
        assertFalse(Vpc.shouldSetDefaultVpcId(previous, desired));
    }

    @Test
    void shouldSetDefaultVpcId_previousNonEmpty_desiredNonEmpty_returnsFalse() {
        List<String> previous = Arrays.asList("sg-123");
        List<String> desired = Arrays.asList("sg-789");
        assertFalse(Vpc.shouldSetDefaultVpcId(previous, desired));
    }

    @Test
    void shouldSetDefaultVpcId_previousNull_desiredNonEmpty_returnsFalse() {
        List<String> previous = null;
        List<String> desired = Arrays.asList("sg-789");
        assertFalse(Vpc.shouldSetDefaultVpcId(previous, desired));
    }

    @Test
    void shouldSetDefaultVpcId_previousEmpty_desiredNonEmpty_returnsFalse() {
        List<String> previous = Collections.emptyList();
        List<String> desired = Arrays.asList("sg-789");
        assertFalse(Vpc.shouldSetDefaultVpcId(previous, desired));
    }
}
