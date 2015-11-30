/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge;

import java.util.Set;

/**
 * For services that depend on an initial member set for bootstrapping.
 */
public interface BootstrappableMembership<MEMBER>
{
    /**
     * Bootstraps a service with an initial set of members.
     *
     * @param members The set of members to bootstrap with.
     */
    void bootstrapWithInitialMembers( Set<MEMBER> members ) throws BootstrapException;

    /**
     * A service can be directly bootstrapped using the previous call
     * or indirectly by some other mechanism, for example by receiving
     * initial member set information over the network. This function
     * merely answers the question whether this instance is currently
     * bootstrapped and can be used for taking appropriate decisions
     * when deciding on whether or not to bootstrap.
     *
     * @return true if the service is already bootstrapped.
     */
    boolean isBootstrapped();
}
