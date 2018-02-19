/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery.procedures;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.discovery.RoleInfo;

public class CoreRoleProcedure extends RoleProcedure
{
    //TODO: Decide whether this should take a topologyService rather than a RaftMachine in order to be consistent with CoreOverviewProcedure. Probably yes?
    private final RaftMachine raft;

    public CoreRoleProcedure( RaftMachine raft )
    {
        super();
        this.raft = raft;
    }

    @Override
    RoleInfo role()
    {
        return raft.isLeader() ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
    }
}
