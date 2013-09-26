package org.elasticsearch.river.mongodb;

import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;

class Status implements Runnable {

	private final MongoDBRiver mongoDBRiver;
	private final MongoDBRiverDefinition definition;
	private final SharedContext context;
	
	public Status(
			MongoDBRiver mongoDBRiver,
			MongoDBRiverDefinition definition,
			SharedContext context) {
		this.mongoDBRiver = mongoDBRiver;
		this.definition = definition;
		this.context = context;
	}
	
	@Override
	public void run() {
		while (true) {
			try {
				if (this.mongoDBRiver.startInvoked) {
					boolean enabled = MongoDBRiverHelper.isRiverEnabled(
							this.mongoDBRiver.client, this.definition.getRiverName());

					if (this.context.isActive() && !enabled) {
						MongoDBRiver.logger.info("About to stop river: {}",
								this.definition.getRiverName());
						this.mongoDBRiver.close();
					}

					if (!this.context.isActive() && enabled) {
						MongoDBRiver.logger.trace("About to start river: {}",
								this.definition.getRiverName());
						this.mongoDBRiver.start();
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