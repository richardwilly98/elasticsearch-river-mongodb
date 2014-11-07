package org.elasticsearch.river.mongodb;

import org.elasticsearch.common.inject.AbstractModule;

public class NodeLevelModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(MongoClientService.class).asEagerSingleton();
    }
}
