package software.amazon.rds.dbsecuritygroup;

public enum IngressStatus {
    Authorizing("authorizing"),
    Authorized("authorized"),
    Revoking("revoking"),
    Revoked("revoked");

    private String value;

    IngressStatus(final String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public Boolean equalsString(final String status) {
        return value.equals(status);
    }
}
