package software.amazon.rds.common.status;

public interface TerminableStatus extends Status {
    boolean isTerminal();
}
