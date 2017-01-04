/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup;

public class CoreServerProtocol extends Protocol<CoreServerProtocol.State>
{
    public CoreServerProtocol()
    {
        super( State.MESSAGE_TYPE );
    }

    public enum State
    {
        MESSAGE_TYPE,
        GET_STORE,
        GET_STORE_ID,
        GET_CORE_SNAPSHOT,
        TX_PULL,

        REGISTER_LOCK_SESSION,
        STOP_LOCK_SESSION,
        END_LOCK_SESSION,

        ACQUIRE_LOCKS,
        RELEASE_LOCK
    }
}
