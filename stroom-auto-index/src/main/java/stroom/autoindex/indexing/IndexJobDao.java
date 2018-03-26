package stroom.autoindex.indexing;

/**
 * Manages access to the index jobs registered in the system.
 * Jobs will be created for each auto index, when job is completed it should be deleted from the database.
 */
public interface IndexJobDao {
    /**
     * Find any existing indexing job for a doc ref, or create a new one using a suggested time window.
     *
     * @param docRefUuid The UUID of the Auto Index doc ref
     * @return The index job found, or next one created.
     * @throws Exception If anything goes wrong
     */
    IndexJob getOrCreate(String docRefUuid) throws Exception;

    /**
     * Given a specific job ID, set the 'started' flag to tre
     * @param jobId The Job ID (randomly allocated when the job is created)
     * @throws Exception If anything goes wrong
     */
    void markAsStarted(String jobId) throws Exception;

    /**
     * Mark a job as complete, it should be deleted from the underlying table and
     * the indexing windows should be updated
     * @param jobId The Job ID
     * @throws Exception If anything goes wrong
     */
    void markAsComplete(String jobId) throws Exception;
}
