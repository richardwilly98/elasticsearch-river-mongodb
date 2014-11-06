package org.elasticsearch.river.mongodb;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

@Singleton
public class MongoClientService extends AbstractLifecycleComponent<MongoClientService> {
    private final ConcurrentMap<List<ServerAddress>, MongoClient> mongoClients = new ConcurrentHashMap<List<ServerAddress>, MongoClient>();

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
        for (MongoClient mongoClient : mongoClients.values()) {
            mongoClient.close();
        }
    }

    /**
     * Get or create a {@link MongoClient} for the given {@code servers}.
     *
     * If a client already exists for the given list of servers it will be reused, otherwise a new client
     * will be created with the provided {@code options}.
     *
     * @param servers
     * @param mongoClientOptions
     * @return
     */
    public MongoClient getMongoClient(List<ServerAddress> servers, MongoClientOptions mongoClientOptions) {
        MongoClient mongoClient = mongoClients.get(servers);
        if (mongoClient == null) {
            logger.info("Creating MongoClient for [{}]", servers);
            mongoClient = new MongoClient(servers, mongoClientOptions);
            MongoClient otherMongoClient = mongoClients.putIfAbsent(servers, mongoClient);
            if (otherMongoClient != null) {
                logger.info("Raced in creating MongoClient for [{}]", servers);
                mongoClient.close();
                mongoClient = otherMongoClient;
            }
        }
        return mongoClient;
    }
}