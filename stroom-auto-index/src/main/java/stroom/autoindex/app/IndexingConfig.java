package stroom.autoindex.app;

public class IndexingConfig {

    private Boolean enabled;

    private int numberOfTasksPerRun = 4;

    private int secondsBetweenChecks = 120;

    public int getNumberOfTasksPerRun() {
        return numberOfTasksPerRun;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public int getSecondsBetweenChecks() {
        return secondsBetweenChecks;
    }

    // Builder like functions.
    public static IndexingConfig asEnabled() {
        final IndexingConfig i = new IndexingConfig();
        i.enabled = true;
        return i;
    }

    public IndexingConfig withNumberOfTasksPerRun(int v) {
        this.numberOfTasksPerRun = v;
        return this;
    }

    public IndexingConfig andSecondsBetweenChecks(int v) {
        this.secondsBetweenChecks = v;
        return this;
    }
}
