package software.amazon.rds.common.status;

public interface StableStatus extends Status {
    boolean isStable();
}
