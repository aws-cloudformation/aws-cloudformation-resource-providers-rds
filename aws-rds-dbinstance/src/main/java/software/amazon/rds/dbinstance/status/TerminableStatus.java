package software.amazon.rds.dbinstance.status;

public interface TerminableStatus extends Status {
    boolean isTerminal();
}
