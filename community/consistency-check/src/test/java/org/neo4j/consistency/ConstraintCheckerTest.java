package org.neo4j.consistency;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.neo_store;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;

public class ConstraintCheckerTest
{
    private void createConstrainedDb( final int nbrOfNodes )
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) graphDatabaseFactory.newImpermanentDatabase( storeDir.absolutePath() );

        Label itemLabel = DynamicLabel.label( "item" );

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( itemLabel ).assertPropertyIsUnique( "id" ).create();
            tx.success();
        }

        for ( int i = 0; i < nbrOfNodes; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( itemLabel ).setProperty( "id", i );
                tx.success();
            }
        }

        db.shutdown();
    }

    private static Config config( File neostoreFile )
    {
        Map<String, String> configMap = new HashMap<>();

        configMap.put( neo_store.name(), neostoreFile.getAbsolutePath() );
        configMap.put( store_dir.name(), neostoreFile.getParentFile().getAbsolutePath() );

        return new Config( configMap );
    }

    private void copyPropertyValue( long fromNode, long toNode )
    {
        File neoStoreFile = new File( storeDir.directory(), NeoStore.DEFAULT_NAME );

        StoreFactory storeFactory = new StoreFactory( config( neoStoreFile ),
                new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(), graphDatabaseFactory.getFileSystem(),
                StringLogger.DEV_NULL, new DefaultTxHook() );

        NeoStore neoStore = storeFactory.newNeoStore( neoStoreFile );

        NodeStore nodeStore = neoStore.getNodeStore();
        PropertyStore propStore = neoStore.getPropertyStore();

        NodeRecord nodeRecord;
        nodeRecord = nodeStore.forceGetRecord( fromNode );
        long[] valueBlocks = propStore.getRecord( nodeRecord.getNextProp() ).getPropertyBlocks().get( 0 )
                .getValueBlocks();

        nodeRecord = nodeStore.forceGetRecord( toNode );
        PropertyRecord propertyRecord = propStore.getRecord( nodeRecord.getNextProp() );
        propertyRecord.getPropertyBlocks().get( 0 ).setValueBlocks( valueBlocks );
        propStore.forceUpdateRecord( propertyRecord );

        neoStore.close();
    }

    @Test
    public void shouldReportThatConstraintsAreOk() throws Exception
    {
        // Given
        createConstrainedDb( 10 );

        // When
        boolean allOk = ConstraintChecker.checkConstraints( fs, storeDir.directory(), testLogger );

        // Then
        assertEquals( true, allOk );
        assertEquals( 0, countInLog( TestLogger.Level.ERROR ) );
    }

    @Test
    public void shouldReportSingleConstraintViolation() throws Exception
    {
        // Given
        createConstrainedDb( 10 );

        // When
        copyPropertyValue( 5, 6 );

        boolean allOk = ConstraintChecker.checkConstraints( fs, storeDir.directory(), testLogger );

        //Then
        assertEquals( false, allOk );
        assertEquals( 1, countInLog( TestLogger.Level.ERROR ) );
    }

    @Test
    public void shouldReportNumerousConstraintViolations() throws Exception
    {
        // Given
        createConstrainedDb( 10 );

        // When
        copyPropertyValue( 1, 2 );
        copyPropertyValue( 1, 3 );
        copyPropertyValue( 3, 4 );

        boolean allOk = ConstraintChecker.checkConstraints( fs, storeDir.directory(), testLogger );

        //Then
        assertEquals( false, allOk );
        assertEquals( 6, countInLog( TestLogger.Level.ERROR ) );
    }

    @Before
    public void before()
    {
        testLogger = new TestLogger();
    }

    @After
    public void after() throws IOException
    {
        FileUtils.deleteRecursively( storeDir.directory() );
    }

    TestLogger testLogger;

    int countInLog( final TestLogger.Level level )
    {
        final int[] logCallCount = {0};

        testLogger.visitLogCalls( new Visitor<TestLogger.LogCall, RuntimeException>()
        {
            @Override
            public boolean visit( TestLogger.LogCall element ) throws RuntimeException
            {
                if ( element.level() == level )
                {
                    logCallCount[0]++;
                }
                return true;
            }
        });

        return logCallCount[0];
    }

    @Rule
    public TargetDirectory.TestDirectory storeDir = TargetDirectory.testDirForTest( ConstraintCheckerTest.class );

    FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    TestGraphDatabaseFactory graphDatabaseFactory = new TestGraphDatabaseFactory().setFileSystem( fs );
}
