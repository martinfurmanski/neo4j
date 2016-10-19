/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.core.state.machines.tx;

import java.io.IOException;

import org.neo4j.kernel.impl.api.CommandVisitor;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.transaction.command.Command;

class PageCachePreloader implements CommandVisitor
{
    private final NeoStores stores;

    PageCachePreloader( NeoStores stores )
    {
        this.stores = stores;
    }

    private boolean preload( Command.BaseCommand command, StoreType storeType )
    {
        ((CommonAbstractStore) stores.getStore( storeType )).isInUse( command.getAfter().getId() );
        return false;
    }

    @Override
    public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
    {
        return preload( command, StoreType.NODE );
    }

    @Override
    public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
    {
        return preload( command, StoreType.RELATIONSHIP );
    }

    @Override
    public boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException
    {
        return preload( command, StoreType.PROPERTY );
    }

    @Override
    public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException
    {
        return preload( command, StoreType.RELATIONSHIP_GROUP );
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitIndexAddNodeCommand( IndexCommand.AddNodeCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( IndexCommand.AddRelationshipCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitIndexRemoveCommand( IndexCommand.RemoveCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitIndexDeleteCommand( IndexCommand.DeleteCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitIndexCreateCommand( IndexCommand.CreateCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitNodeCountsCommand( Command.NodeCountsCommand command ) throws IOException
    {
        return false;
    }

    @Override
    public boolean visitRelationshipCountsCommand( Command.RelationshipCountsCommand command ) throws IOException
    {
        return false;
    }
}
