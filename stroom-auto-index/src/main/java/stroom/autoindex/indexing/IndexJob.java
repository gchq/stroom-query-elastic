package stroom.autoindex.indexing;

import stroom.autoindex.AutoIndexDocRefEntity;
import stroom.autoindex.tracker.TrackerWindow;

import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates a single indexing job, operates on a specific auto index for a specific window of time.
 */
public class IndexJob {
    public static final String TABLE_NAME = "index_job";

    private final String jobId;
    private final AutoIndexDocRefEntity autoIndexDocRefEntity;
    private final TrackerWindow trackerWindow;
    private final long createdTimeMillis;
    private final long startedTimeMillis;

    private IndexJob(final Builder builder) {
        this.jobId = Objects.requireNonNull(builder.jobId);
        this.autoIndexDocRefEntity = Objects.requireNonNull(builder.autoIndexDocRefEntity);
        this.trackerWindow = Objects.requireNonNull(builder.trackerWindow);
        this.createdTimeMillis = builder.createdTimeMillis;
        this.startedTimeMillis = builder.startedTimeMillis;
    }

    public String getJobId() {
        return jobId;
    }

    public AutoIndexDocRefEntity getAutoIndexDocRefEntity() {
        return autoIndexDocRefEntity;
    }

    public TrackerWindow getTrackerWindow() {
        return trackerWindow;
    }

    public long getCreatedTimeMillis() {
        return createdTimeMillis;
    }

    public long getStartedTimeMillis() {
        return startedTimeMillis;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndexJob{");
        sb.append("jobId='").append(jobId).append('\'');
        sb.append(", autoIndexDocRefEntity=").append(autoIndexDocRefEntity);
        sb.append(", trackerWindow=").append(trackerWindow);
        sb.append(", createdTimeMillis=").append(createdTimeMillis);
        sb.append(", startedTimeMillis=").append(startedTimeMillis);
        sb.append('}');
        return sb.toString();
    }

    public static Builder forAutoIndex(final AutoIndexDocRefEntity autoIndexDocRefEntity) {
        return new Builder(autoIndexDocRefEntity);
    }

    public static class Builder {
        private String jobId;
        private final AutoIndexDocRefEntity autoIndexDocRefEntity;
        private TrackerWindow trackerWindow;
        private long createdTimeMillis;
        private long startedTimeMillis;

        private Builder(final AutoIndexDocRefEntity autoIndexDocRefEntity) {
            this.autoIndexDocRefEntity = autoIndexDocRefEntity;
        }

        public Builder jobId(final String value) {
            this.jobId = value;
            return this;
        }

        public Builder trackerWindow(final TrackerWindow value) {
            this.trackerWindow = value;
            return this;
        }

        public Builder createdTimeMillis(final long value) {
            this.createdTimeMillis = value;
            return this;
        }

        public Builder startedTimeMillis(final long value) {
            this.startedTimeMillis = value;
            return this;
        }

        public IndexJob build() {
            return new IndexJob(this);
        }
    }
}
