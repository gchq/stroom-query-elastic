package stroom.autoindex.indexing;

import stroom.autoindex.service.AutoIndexDocRefEntity;

import java.util.Optional;

/**
 * Manages access to the index jobs registered in the system.
 * Jobs will be created for each auto index, when job is completed it should be deleted from the database.
 */
public interface IndexJobDao {
    /**
     * Find any existing indexing job for a doc ref, or create a new one using a suggested time window.
     * If the timeline has been exhausted, this will return an {@link Optional#empty()}
     * @param autoIndexDocRefEntity The Auto Index Doc Ref Entity that we are fetching jobs for
     * @return The index job found, or next one created. Or empty if the timeline has been exhausted.
     */
    Optional<IndexJob> getOrCreate(AutoIndexDocRefEntity autoIndexDocRefEntity);

    /**
     * Get a specific index job by it's job id.
     *
     * @param jobId The Job ID to find
     * @return IndexJob, Optional so if the job cannot be found, will be missing.
     */
    Optional<IndexJob> get(String jobId);

    /**
     * Given a specific job ID, set the 'started' flag to tre
     * @param jobId The Job ID (randomly allocated when the job is created)
     */
    void markAsStarted(String jobId);

    /**
     * Mark a job as complete, it should be deleted from the underlying table and
     * the indexing windows should be updated
     * @param jobId The Job ID
     */
    void markAsComplete(String jobId);
}
