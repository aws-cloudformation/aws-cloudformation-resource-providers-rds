package software.amazon.rds.common.status;

/**
 * Represents a status that could be considered "terminal" for a resource.
 * Terminal states cause resource stabilization to fail immediately. If a
 * resource enters a terminal state following a mutation, the operation fails
 * with a {@link software.amazon.cloudformation.exceptions.CfnNotStabilizedException}.
 */
public interface TerminableStatus extends Status {
    /**
     * Returns true if the status is terminal.
     */
    boolean isTerminal();
}
