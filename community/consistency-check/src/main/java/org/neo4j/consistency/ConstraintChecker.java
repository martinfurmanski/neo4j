package org.neo4j.consistency;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.AbstractSchemaRule;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.lang.System.getProperty;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.arrays_mapped_memory_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.neo_store;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.nodestore_mapped_memory_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.nodestore_propertystore_index_keys_mapped_memory_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.nodestore_propertystore_index_mapped_memory_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.relationshipstore_mapped_memory_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.strings_mapped_memory_size;

// TODO: Verify schema constraints, see below.

/* A constraint only checker.
*
*  Useful until the entire consistency checker has been optimized for speed and configurability.
*/
public class ConstraintChecker
{
    private static final long PAGE_SIZE_MASK = 0xFFF;

    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 1 )
        {
            System.out.println( "Expected one argument: <graph.db>" );
        }

        long startTime = System.currentTimeMillis();

        File storeDir = new File( args[0] ).getCanonicalFile();
        boolean allOk = checkConstraints( new DefaultFileSystemAbstraction(), storeDir, StringLogger.SYSTEM );

        long endTime = System.currentTimeMillis();

        System.out.println( String.format( "Done in %d ms : Result = %s", (endTime - startTime),
                allOk ? "SUCCESS" : "FAILURE" ) );
        System.exit( allOk ? 0 : -1 );
    }

    public static boolean checkConstraints( FileSystemAbstraction fs, File storeDir, StringLogger log )
    {
        boolean allOk = true;

        File neoStoreFile = new File( storeDir, NeoStore.DEFAULT_NAME );

        StoreFactory storeFactory = new StoreFactory( config( storeDir, neoStoreFile, true ),
                new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(), fs,
                log, new DefaultTxHook() );

        NeoStore neoStore = storeFactory.newNeoStore( neoStoreFile );

        NodeStore nodeStore = neoStore.getNodeStore();
        PropertyStore propStore = neoStore.getPropertyStore();

        Iterator<SchemaRule> schemaRuleIterator = neoStore.getSchemaStore().iterator();
        while ( schemaRuleIterator.hasNext() )
        {
            SchemaRule schemaRule = schemaRuleIterator.next();
            if ( schemaRule.getKind().equals( SchemaRule.Kind.UNIQUENESS_CONSTRAINT ) )
            {
                UniquenessConstraintRule uniquenessConstraintRule = (UniquenessConstraintRule) schemaRule;

                int labelId = uniquenessConstraintRule.getLabel();
                int propertyKey = uniquenessConstraintRule.getPropertyKey();

                if ( !verifyUniqueness( nodeStore, propStore, labelId, propertyKey, log ) )
                {
                    allOk = false;
                }
            }
            else if( schemaRule.getKind().equals( SchemaRule.Kind.CONSTRAINT_INDEX_RULE ) )
            {
                IndexRule indexRule = (IndexRule) schemaRule;

                int labelId = indexRule.getLabel();
                int propertyKey = indexRule.getPropertyKey();

                // TODO: Get owning constraint type and verify that it holds.
            }
        }

        neoStore.close();
        return allOk;
    }

    private static boolean verifyUniqueness( NodeStore nodeStore, PropertyStore propStore, int labelId, int propertyKey, StringLogger log )
    {
        long nodeHighId = nodeStore.getHighId();

        class NonUniqueCandidate
        {
            long nodeId;
            long[] valueBlocks;

            NonUniqueCandidate( long nodeId, long[] valueBlocks )
            {
                this.nodeId = nodeId;
                this.valueBlocks = valueBlocks;
            }
        }

        class NonUniqueCandidates
        {
            long propertyKey;
            ArrayList<NonUniqueCandidate> list = new ArrayList<>();
        }

        PrimitiveIntSet valueBlocksHashSet = Primitive.offHeapIntSet( 64*1024*1024 );

        TreeMap<Integer/*valueBlocksHash*/,NonUniqueCandidates> candidateValueListMap = new TreeMap<>();

        long nodeId = 0;
        boolean firstPass = true; // Two-pass is a memory conservation strategy. Otherwise we would need to keep track of an initial node for all value-hashes up-front.
        while ( nodeId < nodeHighId )
        {
            NodeRecord nodeRecord = nodeStore.forceGetRecord( nodeId );
            long labels[] = NodeLabelsField.parseLabelsField( nodeRecord ).get( nodeStore );

            long nodeLabelKeyMatchCount = 0;

            for ( int i = 0; i < labels.length; i++ )
            {
                if ( labels[i] == labelId )
                {
                    long propId = nodeRecord.getNextProp();
                    while ( propId != Record.NO_NEXT_PROPERTY.intValue() )
                    {
                        PropertyRecord record = propStore.forceGetRecord( propId );
                        List<PropertyBlock> propertyBlocks = record.getPropertyBlocks();

                        for ( PropertyBlock propertyBlock : propertyBlocks )
                        {
                            if ( propertyBlock.getKeyIndexId() == propertyKey )
                            {
                                if ( firstPass )
                                {
                                    nodeLabelKeyMatchCount++;
                                    if ( nodeLabelKeyMatchCount > 1 )
                                    {
                                        log.error( String.format( "Node(%d) has internal duplicate matches for label(%d) propKey(%d)",
                                                nodeId, labelId, propertyKey ) );
                                    }
                                }

                                long[] valueBlocks = propertyBlock.getValueBlocks();
                                int valueBlocksHash = Arrays.hashCode( valueBlocks );

                                if ( firstPass )
                                {
                                    boolean unique = valueBlocksHashSet.add( valueBlocksHash ); /* uniqueness of _hash_ */

                                    if ( !unique )
                                    {
                                        if( !candidateValueListMap.containsKey( valueBlocksHash ))
                                        {
                                            candidateValueListMap.put( valueBlocksHash, new NonUniqueCandidates() );
                                            /* non-unique candidates populated during second pass */
                                        }
                                    }
                                }
                                else /* second pass */
                                {
                                    NonUniqueCandidates nonUniqueCandidates = candidateValueListMap.get( valueBlocksHash );
                                    if ( nonUniqueCandidates != null )
                                    {
                                        nonUniqueCandidates.list.add( new NonUniqueCandidate( nodeId, valueBlocks ) );
                                    }
                                }
                            }
                        }

                        propId = record.getNextProp();
                    }
                }
            }

            nodeId++;
            if( firstPass && nodeId == nodeHighId )
            {
                firstPass = false;
                nodeId = 0; /* reset for second pass */
            }
        }

        boolean allOk = true;
        for ( NonUniqueCandidates nonUniqueCandidatesContainer : candidateValueListMap.values() )
        {
            ArrayList<NonUniqueCandidate> list = nonUniqueCandidatesContainer.list;

            for ( int i = 0; i < list.size(); i++ )
            {
                for ( int j = i+1; j < list.size(); j++ )
                {
                    NonUniqueCandidate A = list.get( i );
                    NonUniqueCandidate B = list.get( j );

                    if ( Arrays.equals( A.valueBlocks, B.valueBlocks ))
                    {
                        allOk = false;

                        log.error( String.format( "Node(%d) and Node(%d) have the same value for propertyKey(%d)",
                                A.nodeId, B.nodeId,
                                nonUniqueCandidatesContainer.propertyKey ) );
                    }
                }
            }
        }

        return allOk;
    }

    private static void setMMConfigForFile( Map<String, String> configMap, File mmBaseDir, Setting<Long> setting )
    {
        String fileName = setting.name().substring( 0, setting.name().lastIndexOf( ".mapped_memory" ) );
        File file = new File( mmBaseDir, fileName );

        long mm_size = (file.length() + PAGE_SIZE_MASK) & ~PAGE_SIZE_MASK;

        configMap.put( setting.name(), getProperty( setting.name(), String.valueOf( mm_size ) ) );
    }

    private static Config config( File baseDir, File neostoreFile, boolean read_only_db )
    {
        Map<String, String> configMap = new HashMap<>();

        setMMConfigForFile( configMap, baseDir, nodestore_mapped_memory_size );
        setMMConfigForFile( configMap, baseDir, relationshipstore_mapped_memory_size );
        setMMConfigForFile( configMap, baseDir, nodestore_propertystore_mapped_memory_size );
        setMMConfigForFile( configMap, baseDir, nodestore_propertystore_index_mapped_memory_size );
        setMMConfigForFile( configMap, baseDir, nodestore_propertystore_index_keys_mapped_memory_size );
        setMMConfigForFile( configMap, baseDir, strings_mapped_memory_size );
        setMMConfigForFile( configMap, baseDir, arrays_mapped_memory_size );

        configMap.put( read_only.name(),
                getProperty( read_only.name(), Boolean.toString( read_only_db ) ) );

        configMap.put( neo_store.name(), neostoreFile.getAbsolutePath() );
        configMap.put( store_dir.name(), neostoreFile.getParentFile().getAbsolutePath() );

        return new Config( configMap );
    }
}
