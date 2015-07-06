package org.elasticsearch.river.mongodb;

import org.elasticsearch.river.AbstractRiverComponent;

public abstract class MongoDBRiverComponent extends AbstractRiverComponent {

    protected MongoDBRiverComponent(MongoDBRiver river) {
        super(river.riverName(), river.settings());
    }
}
