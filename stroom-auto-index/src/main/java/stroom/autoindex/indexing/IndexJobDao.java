package stroom.autoindex.indexing;

import java.util.List;

/**
 * Manages access to the index jobs registered in the system.
 * Jobs will be created for each auto index, when job is completed it should be deleted from the database.
 */
public interface IndexJobDao {
    /**
     *
     * @param docRefUuid
     * @return
     */
    IndexJob getOrCreate(String docRefUuid) throws Exception;
    List<IndexJob> getAll();

}
