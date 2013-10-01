package org.elasticsearch.river.mongodb;

import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;

class StatusChecker implements Runnable {

    private final MongoDBRiver mongoDBRiver;
    private final MongoDBRiverDefinition definition;
    private final SharedContext context;

    public StatusChecker(MongoDBRiver mongoDBRiver, MongoDBRiverDefinition definition, SharedContext context) {
        this.mongoDBRiver = mongoDBRiver;
        this.definition = definition;
        this.context = context;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (this.mongoDBRiver.startInvoked) {
                    Status status = MongoDBRiverHelper.getRiverStatus(this.mongoDBRiver.client, this.definition.getRiverName());
                    if (status != this.context.getStatus()) {
                        if (status == Status.RUNNING) {
                            MongoDBRiver.logger.info("About to stop river: {}", this.definition.getRiverName());
                            this.mongoDBRiver.close();                            
                        } else if (status == Status.STOPPED) {
                            MongoDBRiver.logger.trace("About to start river: {}", this.definition.getRiverName());
                            this.mongoDBRiver.start();                            
                        }
                    }
                }
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                MongoDBRiver.logger.info("Status thread interrupted", e, (Object) null);
                Thread.currentThread().interrupt();
                break;
            }

        }
    }
}
