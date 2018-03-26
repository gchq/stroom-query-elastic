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
    private final boolean started;

    private IndexJob(final Builder builder) {
        this.jobId = Objects.requireNonNull(builder.jobId);
        this.autoIndexDocRefEntity = Objects.requireNonNull(builder.autoIndexDocRefEntity);
        this.trackerWindow = Objects.requireNonNull(builder.trackerWindow);
        this.createdTimeMillis = builder.createdTimeMillis;
        this.started = builder.started;
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

    public boolean isStarted() {
        return started;
    }

    public static Builder forAutoIndex(final AutoIndexDocRefEntity autoIndexDocRefEntity) {
        return new Builder(autoIndexDocRefEntity);
    }

    public static class Builder {
        private String jobId;
        private final AutoIndexDocRefEntity autoIndexDocRefEntity;
        private TrackerWindow trackerWindow;
        private long createdTimeMillis;
        private boolean started;

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

        public Builder started(final boolean value) {
            this.started = value;
            return this;
        }

        public IndexJob build() {
            return new IndexJob(this);
        }
    }
}
