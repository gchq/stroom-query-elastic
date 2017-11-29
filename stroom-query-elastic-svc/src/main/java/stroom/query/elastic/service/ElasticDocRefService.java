package stroom.query.elastic.service;

import stroom.query.audit.DocRefException;
import stroom.query.elastic.hibernate.ElasticIndexConfig;

import java.util.List;
import java.util.Optional;

public interface ElasticDocRefService {
    String STROOM_INDEX_NAME = "stroom";
    String DOC_REF_INDEXED_TYPE = "docref";

    /**
     * Retrieve all of the index entities currently registered
     * @return The list of all known index entities
     * @throws DocRefException
     */
    List<ElasticIndexConfig> getAll() throws DocRefException;

    /**
     * Retrieve the full config for the given DocRef
     * @param uuid              The UUID of the docRef to return
     * @return                  The full implementation specific config for this docRef.
     * @throws DocRefException  If something goes wrong
     */
    Optional<ElasticIndexConfig> get(String uuid) throws DocRefException;

    /**
     * A new document has been created in Stroom
     *
     * @param uuid              The UUID of the document as created by stroom
     * @param name              The name of the document to be created.
     * @return The new index entity
     * @throws DocRefException  If something goes wrong
     */
    Optional<ElasticIndexConfig> createDocument(String uuid, String name) throws DocRefException;

    /**
     * Used to update a specific elastic index config.
     * This will be used by our user interface to configure the underlying index and the stroom references to it
     * @param uuid The UUID of DocRef used to store the index configuration
     * @param updatedConfig The updated configuration
     * @return
     * @throws DocRefException
     */
    Optional<ElasticIndexConfig> update(String uuid, ElasticIndexConfig updatedConfig) throws DocRefException;

    /**
     * A notification from Stroom that a document is being copied. The external system should
     * copy it's configuration for the original into a new entity.
     *
     * @param originalUuid      The uuid of the document being copied
     * @param copyUuid          The uuid of the copy
     * @return The new index entity
     * @throws DocRefException  If something goes wrong
     */
    Optional<ElasticIndexConfig> copyDocument(String originalUuid, String copyUuid) throws DocRefException;

    /**
     * A Notification from Stroom that the document has been 'moved'. In most cases the external system
     * will not care about this.
     *
     * @param uuid             The uuid of the document that was moved
     * @return The updated index entity
     * @throws DocRefException  If something goes wrong
     */
    Optional<ElasticIndexConfig> documentMoved(String uuid) throws DocRefException;

    /**
     * A notifiation from Stroom that the name of a document has been changed. Whilst the name belongs to stroom
     * it may be helpful for the external system to know what the name is, but the name should not be used for referencing
     * the DocRef between systems as it could easily be out of sync.
     *
     * @param uuid The uuid of the document you want to rename.
     * @param name The new name of the document.
     * @return The updated index entity
     * @throws DocRefException  If something goes wrong
     */
    Optional<ElasticIndexConfig> documentRenamed(String uuid, String name) throws DocRefException;

    /**
     * The document with this UUID is being deleted in Stroom.
     *
     * @param uuid The uuid of the document you want to delete.
     * @throws DocRefException  If something goes wrong
     */
    void deleteDocument(String uuid) throws DocRefException;
}
