package org.neo4j.coreedge;

public class BootstrapException extends Exception
{
    public BootstrapException( Throwable cause )
    {
        super( cause );
    }

    public BootstrapException( String message )
    {
        super( message );
    }
}
