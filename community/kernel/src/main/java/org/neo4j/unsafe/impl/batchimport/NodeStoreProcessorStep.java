/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Convenient step for processing all in use {@link NodeRecord records} in the {@link NodeStore node store}.
 */
public class NodeStoreProcessorStep extends StoreProcessorStep<NodeRecord>
{
    private final NodeStore nodeStore;

    protected NodeStoreProcessorStep( StageControl control, String name, Configuration config, NodeStore nodeStore,
            StoreProcessor<NodeRecord> processor )
    {
        super( control, name, config.batchSize(), config.movingAverageSize(), nodeStore, processor, false );
        this.nodeStore = nodeStore;
    }

    @Override
    protected NodeRecord loadRecord( long id, NodeRecord into )
    {
        return nodeStore.loadRecord( id, into );
    }

    @Override
    protected NodeRecord createReusableRecord()
    {
        return new NodeRecord( -1 );
    }
}
