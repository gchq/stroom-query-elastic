package stroom.akka.query.cluster;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import stroom.akka.query.actors.QueryDataSourceActor;
import stroom.akka.query.actors.QuerySearchActor;
import stroom.query.audit.service.QueryService;
import stroom.security.ServiceUser;

import javax.xml.ws.Service;

import static stroom.akka.query.cluster.QuerySearchFrontendMain.CLUSTER_SYSTEM_NAME;

public class QuerySearchBackendMain {
    private final ServiceUser user;
    private final QueryService service;
    private final int port;

    public static class Builder {
        private final int port;
        private ServiceUser user;
        private QueryService service;

        public Builder(int port) {
            this.port = port;
        }

        public Builder withService(final QueryService service) {
            this.service = service;
            return this;
        }

        public Builder asUser(final ServiceUser user) {
            this.user = user;
            return this;
        }

        public QuerySearchBackendMain build() {
            return new QuerySearchBackendMain(user, service, this.port);

        }
    }

    public static Builder onPort(final int port) {
        return new Builder(port);
    }

    private QuerySearchBackendMain(final ServiceUser user,
                                   final QueryService service,
                                   final int port) {
        this.user = user;
        this.service = service;
        this.port = port;
    }

    public void run() {
        final Config config =
                ConfigFactory.parseString(
                        "akka.remote.netty.tcp.port=" + port + "\n" +
                        "akka.remote.artery.canonical.port=" + port).
                    withFallback(ConfigFactory.parseString("akka.cluster.roles = [backend]")).
                    withFallback(ConfigFactory.load("search"));

        ActorSystem system = ActorSystem.create(CLUSTER_SYSTEM_NAME, config);

        system.actorOf(QuerySearchBackend.props(user, service), "querySearchBackend");
    }
}
