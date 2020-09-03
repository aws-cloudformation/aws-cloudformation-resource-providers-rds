package software.amazon.rds.optiongroup;

public class OptionVersion implements Comparable<OptionVersion> {

    protected String version;

    public OptionVersion(final String version) {
        this.version = version;
    }

    @Override
    public int compareTo(OptionVersion other) {
        //version string are dot connected with a ending like .v[0-9]+
        //eg:      5.1.2.v1      4.2.6.v1
        String[] vals1 = version.split("\\.");
        String[] vals2 = other.version.split("\\.");
        int i = 0;
        //ignore the last non-digital string
        final int len1 = vals1.length - 1;
        final int len2 = vals2.length - 1;
        while (i < len1 && i < len2 && vals1[i].equals(vals2[i])) {
            i++;
        }
        //Compare the first non-equal digital string
        if (i < len1 && i < len2) {
            return Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
        }
        //return true if the first version is smaller to the second one
        //case: 1.1 > 1.1.1
        return Integer.compare(len1, len2);
    }
}
