package org.elasticsearch.river.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

@Singleton
public class MongoClientService extends AbstractLifecycleComponent<MongoClientService> {

    private final Map<ClientCacheKey, MongoClient> mongoClients = new HashMap<>();

    private final Object $lock = new Object[0];

    @Inject
    public MongoClientService(Settings settings) {
        super(settings);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        synchronized ($lock) {
            for (MongoClient mongoClient : mongoClients.values()) {
                mongoClient.close();
            }
            mongoClients.clear();
        }
    }

    /**
     * Get or create a {@link MongoClient} for the given {@code servers}.
     *
     * If a client already exists for the given list of servers with the same credentials and
     * options it will be reused, otherwise a new client will be created.
     */
    public MongoClient getMongoClient(MongoDBRiverDefinition definition, List<ServerAddress> overrideServers) {
        synchronized ($lock) {
            List<ServerAddress> servers = overrideServers != null ? overrideServers : definition.getMongoServers();
            MongoClient mongoClient = mongoClients.get(servers);
            if (mongoClient != null) {
                return mongoClient;
            }

            logger.info("Creating MongoClient for [{}]", servers);
            List<MongoCredential> mongoCredentials = new ArrayList<>();
            if (!Strings.isNullOrEmpty(definition.getMongoLocalUser()) && !Strings.isNullOrEmpty(definition.getMongoLocalPassword())) {
                mongoCredentials.add(MongoCredential.createMongoCRCredential(
                        definition.getMongoLocalUser(),
                        !Strings.isNullOrEmpty(definition.getMongoLocalAuthDatabase()) ? definition.getMongoLocalAuthDatabase() : MongoDBRiver.MONGODB_LOCAL_DATABASE,
                        definition.getMongoLocalPassword().toCharArray()));
            }
            if (!Strings.isNullOrEmpty(definition.getMongoAdminUser()) && !Strings.isNullOrEmpty(definition.getMongoAdminPassword())) {
                mongoCredentials.add(MongoCredential.createMongoCRCredential(
                        definition.getMongoAdminUser(),
                        !Strings.isNullOrEmpty(definition.getMongoAdminAuthDatabase()) ? definition.getMongoAdminAuthDatabase() : MongoDBRiver.MONGODB_ADMIN_DATABASE,
                        definition.getMongoAdminPassword().toCharArray()));
            }
            MongoClientOptions mongoClientOptions = definition.getMongoClientOptions();
            mongoClient = new MongoClient(servers, mongoCredentials, mongoClientOptions);
            mongoClients.put(new ClientCacheKey(servers, mongoCredentials, mongoClientOptions), mongoClient);
            return mongoClient;
        }
    }

    static class ClientCacheKey {

        private final List<ServerAddress> servers;
        private final List<MongoCredential> mongoCredentials;
        private final MongoClientOptions mongoClientOptions;

        public ClientCacheKey(List<ServerAddress> servers, List<MongoCredential> mongoCredentials, MongoClientOptions mongoClientOptions) {
            this.servers = servers;
            this.mongoCredentials = mongoCredentials;
            this.mongoClientOptions = mongoClientOptions;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((mongoClientOptions == null) ? 0 : mongoClientOptions.hashCode());
            result = prime * result + ((mongoCredentials == null) ? 0 : mongoCredentials.hashCode());
            result = prime * result + ((servers == null) ? 0 : servers.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ClientCacheKey other = (ClientCacheKey) obj;
            if (mongoClientOptions == null) {
                if (other.mongoClientOptions != null)
                    return false;
            } else if (!mongoClientOptions.equals(other.mongoClientOptions))
                return false;
            if (mongoCredentials == null) {
                if (other.mongoCredentials != null)
                    return false;
            } else if (!mongoCredentials.equals(other.mongoCredentials))
                return false;
            if (servers == null) {
                if (other.servers != null)
                    return false;
            } else if (!servers.equals(other.servers))
                return false;
            return true;
        }

    }

}
