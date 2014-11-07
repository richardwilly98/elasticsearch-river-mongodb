/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.river.mongodb;

import java.util.Collection;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.rest.action.mongodb.RestMongoDBRiverAction;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.mongodb.MongoClientService;
import org.elasticsearch.river.mongodb.NodeLevelModule;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.MongoDBRiverModule;

import com.google.common.collect.ImmutableList;

/**
 * @author flaper87 (Flavio Percoco Premoli)
 * @author aparo (Alberto Paro)
 * @author kryptt (Rodolfo Hansen)
 */

public class MongoDBRiverPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return MongoDBRiver.NAME;
    }

    @Override
    public String description() {
        return MongoDBRiver.DESCRIPTION;
    }
    
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        return ImmutableList.<Class<? extends LifecycleComponent>>builder().addAll(super.services()).add(MongoClientService.class).build();
    }

    /**
     * Node-level modules
     */
    @Override
    public Collection<Class<? extends Module>> modules() {
        return ImmutableList.<Class<? extends Module>>builder().addAll(super.modules()).add(NodeLevelModule.class).build();
    }

    /**
     * Register the MongoDB river
     */
    public void onModule(RiversModule module) {
        module.registerRiver(MongoDBRiver.TYPE, MongoDBRiverModule.class);
    }

    /**
     * Register the REST handler
     */
    public void onModule(RestModule module) {
        module.addRestAction(RestMongoDBRiverAction.class);
    }

}
