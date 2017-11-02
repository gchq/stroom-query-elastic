package stroom.query.elastic.hibernate;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import stroom.query.api.v2.DocRef;

import javax.inject.Inject;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.Optional;

public class ElasticIndexConfigServiceImpl implements ElasticIndexConfigService {
    private final SessionFactory database;

    @Inject
    public ElasticIndexConfigServiceImpl(final SessionFactory database) {
        this.database = database;
    }

    @Override
    public Optional<ElasticIndexConfig> get(final DocRef docRef) {
        if (null == docRef) {
            return null;
        }

        try (final Session session = database.openSession()) {
            final CriteriaBuilder cb = session.getCriteriaBuilder();

            final CriteriaQuery<ElasticIndexConfig> cq = cb.createQuery(ElasticIndexConfig.class);
            final Root<ElasticIndexConfig> root = cq.from(ElasticIndexConfig.class);
            cq.where(cb.equal(root.get(ElasticIndexConfig.UUID), docRef.getUuid()));

            return session.createQuery(cq).getResultList().stream().findFirst();
        }
    }
}
