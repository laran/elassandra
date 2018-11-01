/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains some code from Elasticsearch (http://www.elastic.co)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.cluster.service;


import ch.qos.logback.classic.jmx.JMXConfiguratorMBean;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.joran.spi.JoranException;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.TableAttributes;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ElassandraDaemon;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.elassandra.ConcurrentMetaDataUpdateException;
import org.elassandra.NoPersistedMetaDataException;
import org.elassandra.cluster.QueryManager;
import org.elassandra.cluster.SchemaManager;
import org.elassandra.cluster.Serializer;
import org.elassandra.cluster.routing.AbstractSearchStrategy;
import org.elassandra.cluster.routing.PrimaryFirstSearchStrategy;
import org.elassandra.discovery.CassandraDiscovery;
import org.elassandra.index.ExtendedElasticSecondaryIndex;
import org.elassandra.index.search.TokenRangesService;
import org.elassandra.indices.CassandraSecondaryIndicesApplier;
import org.elassandra.shard.CassandraShardStartedBarrier;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.DiscoveryNodeStatus;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import static java.util.Collections.emptyList;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.elasticsearch.common.settings.Setting.listSetting;


public class ClusterService extends BaseClusterService {

    public static final String ELASTIC_ADMIN_KEYSPACE = "elastic_admin";
    public static final String ELASTIC_ADMIN_METADATA_TABLE = "metadata";

    public static final String SETTING_CLUSTER_DATACENTER_GROUP = "datacenter.group";
    public static final Setting<List<String>> SETTING_DATCENTER_GROUP = listSetting(SETTING_CLUSTER_DATACENTER_GROUP, emptyList(), Function.identity(), Property.NodeScope);

    // settings levels : system, cluster, index, table(_meta)
    public static final String SYSTEM_PREFIX = "es.";
    public static final String CLUSTER_PREFIX = "cluster.";
    public static final String INDEX_PREFIX = "index.";
    public static final String TABLE_PREFIX = "";
    private static final int CREATE_ELASTIC_ADMIN_RETRY_ATTEMPTS = Integer.getInteger(SYSTEM_PREFIX + "create_elastic_admin_retry_attempts", 5);

    /**
     * Dynamic mapping update timeout
     */
    public static final String MAPPING_UPDATE_TIMEOUT = "mapping_update_timeout";

    /**
     * Secondary index class
     */
    public static final String SECONDARY_INDEX_CLASS = "secondary_index_class";

    /**
     * Search strategy class
     */
    public static final String SEARCH_STRATEGY_CLASS = "search_strategy_class";

    /**
     * When true, add the cassandra node id to documents (for use with the token aggregation feature)
     */
    public static final String INCLUDE_NODE_ID       = "include_node_id";

    /**
     * When true, re-indexes a row when compacting, usefull to delete expired documents or columns.
     */
    public static final String INDEX_ON_COMPACTION   = "index_on_compaction";

    /**
     * When true, refreshes ES index after each update (used for testing).
     */
    public static final String SYNCHRONOUS_REFRESH   = "synchronous_refresh";

    /**
     * When true, delete kespace/table when removing an index.
     */
    public static final String DROP_ON_DELETE_INDEX  = "drop_on_delete_index";

    /**
     * When true, snapshot lucene files with sstables.
     */
    public static final String SNAPSHOT_WITH_SSTABLE = "snapshot_with_sstable";

    /**
     * When true, use the optimized version less Elasticsearch engine.
     */
    public static final String VERSION_LESS_ENGINE   = "version_less_engine";

    /**
     * Lucene numeric precision to store _token , see http://blog-archive.griddynamics.com/2014/10/numeric-range-queries-in-lucenesolr.html
     */
    public static final String TOKEN_PRECISION_STEP  = "token_precision_step";

    /**
     * Enable the token_ranges bitset cache (cache the token_ranges filter result at the lucene liveDocs level).
     */
    public static final String TOKEN_RANGES_BITSET_CACHE    = "token_ranges_bitset_cache";

    /**
     * Expiration time for unused cached token_ranges queries.
     */
    public static final String TOKEN_RANGES_QUERY_EXPIRE = "token_ranges_query_expire";

    /**
     * Add static columns to indexed documents (default is false).
     */
    public static final String INDEX_STATIC_COLUMNS = "index_static_columns";

    /**
     * Index only static columns (one document per partition row, ex: timeseries tags).
     */
    public static final String INDEX_STATIC_ONLY = "index_static_only";

    /**
     * Index static document (document containing only static columns + partion keys).
     */
    public static final String INDEX_STATIC_DOCUMENT = "index_static_document";

    /**
     * Index in insert-only mode without a read-before-write
     */
    public static final String INDEX_INSERT_ONLY = "index_insert_only";

    // system property settings
    public static final String SETTING_SYSTEM_MAPPING_UPDATE_TIMEOUT = SYSTEM_PREFIX+MAPPING_UPDATE_TIMEOUT;
    public static final String SETTING_SYSTEM_SECONDARY_INDEX_CLASS = SYSTEM_PREFIX+SECONDARY_INDEX_CLASS;
    public static final String SETTING_SYSTEM_SEARCH_STRATEGY_CLASS = SYSTEM_PREFIX+SEARCH_STRATEGY_CLASS;
    public static final String SETTING_SYSTEM_INCLUDE_NODE_ID = SYSTEM_PREFIX+INCLUDE_NODE_ID;
    public static final String SETTING_SYSTEM_INDEX_ON_COMPACTION = SYSTEM_PREFIX+INDEX_ON_COMPACTION;
    public static final String SETTING_SYSTEM_SYNCHRONOUS_REFRESH = SYSTEM_PREFIX+SYNCHRONOUS_REFRESH;
    public static final String SETTING_SYSTEM_DROP_ON_DELETE_INDEX = SYSTEM_PREFIX+DROP_ON_DELETE_INDEX;
    public static final String SETTING_SYSTEM_SNAPSHOT_WITH_SSTABLE = SYSTEM_PREFIX+SNAPSHOT_WITH_SSTABLE;
    public static final String SETTING_SYSTEM_VERSION_LESS_ENGINE = SYSTEM_PREFIX+VERSION_LESS_ENGINE;
    public static final String SETTING_SYSTEM_TOKEN_PRECISION_STEP = SYSTEM_PREFIX+TOKEN_PRECISION_STEP;
    public static final String SETTING_SYSTEM_TOKEN_RANGES_BITSET_CACHE = SYSTEM_PREFIX+TOKEN_RANGES_BITSET_CACHE;
    public static final String SETTING_SYSTEM_TOKEN_RANGES_QUERY_EXPIRE = SYSTEM_PREFIX+TOKEN_RANGES_QUERY_EXPIRE;

    // elassandra cluster settings
    public static final String SETTING_CLUSTER_MAPPING_UPDATE_TIMEOUT = CLUSTER_PREFIX+MAPPING_UPDATE_TIMEOUT;
    public static final String SETTING_CLUSTER_SECONDARY_INDEX_CLASS = CLUSTER_PREFIX+SECONDARY_INDEX_CLASS;
    public static final String SETTING_CLUSTER_SEARCH_STRATEGY_CLASS = CLUSTER_PREFIX+SEARCH_STRATEGY_CLASS;
    public static final String SETTING_CLUSTER_INCLUDE_NODE_ID = CLUSTER_PREFIX+INCLUDE_NODE_ID;
    public static final String SETTING_CLUSTER_INDEX_ON_COMPACTION = CLUSTER_PREFIX+INDEX_ON_COMPACTION;
    public static final String SETTING_CLUSTER_SYNCHRONOUS_REFRESH = CLUSTER_PREFIX+SYNCHRONOUS_REFRESH;
    public static final String SETTING_CLUSTER_DROP_ON_DELETE_INDEX = CLUSTER_PREFIX+DROP_ON_DELETE_INDEX;
    public static final String SETTING_CLUSTER_SNAPSHOT_WITH_SSTABLE = CLUSTER_PREFIX+SNAPSHOT_WITH_SSTABLE;
    public static final String SETTING_CLUSTER_VERSION_LESS_ENGINE = CLUSTER_PREFIX+VERSION_LESS_ENGINE;
    public static final String SETTING_CLUSTER_TOKEN_PRECISION_STEP = CLUSTER_PREFIX+TOKEN_PRECISION_STEP;
    public static final String SETTING_CLUSTER_TOKEN_RANGES_BITSET_CACHE = CLUSTER_PREFIX+TOKEN_RANGES_BITSET_CACHE;

    public static int defaultPrecisionStep = Integer.getInteger(SETTING_SYSTEM_TOKEN_PRECISION_STEP, 6);

    public static class DocPrimaryKey {
        public String[] names;
        public Object[] values;
        public boolean isStaticDocument; // pk = partition key and pk has clustering key.

        public DocPrimaryKey(String[] names, Object[] values, boolean isStaticDocument) {
            this.names = names;
            this.values = values;
            this.isStaticDocument = isStaticDocument;
        }

        public DocPrimaryKey(String[] names, Object[] values) {
            this.names = names;
            this.values = values;
            this.isStaticDocument = false;
        }

        public List<ByteBuffer> serialize(ParsedStatement.Prepared prepared) {
            List<ByteBuffer> boundValues = new ArrayList<ByteBuffer>(values.length);
            for (int i = 0; i < values.length; i++) {
                Object v = values[i];
                AbstractType type = prepared.boundNames.get(i).type;
                boundValues.add(v instanceof ByteBuffer || v == null ? (ByteBuffer) v : type.decompose(v));
            }
            return boundValues;
        }

        @Override
        public String toString() {
            return Serializer.stringify(values, values.length);
        }
    }

    private MetaStateService metaStateService;
    private IndicesService indicesService;
    private CassandraDiscovery discovery;

    private final TokenRangesService tokenRangeService;
    private final CassandraSecondaryIndicesApplier cassandraSecondaryIndicesApplier;

    // manage asynchronous CQL schema update
    protected final AtomicReference<MetadataSchemaUpdate> lastMetadataToSave = new AtomicReference<MetadataSchemaUpdate>(null);
    protected final Semaphore metadataToSaveSemaphore = new Semaphore(0);

    protected final MappingUpdatedAction mappingUpdatedAction;

    public final static Class<? extends Index> defaultSecondaryIndexClass = ExtendedElasticSecondaryIndex.class;

    protected final PrimaryFirstSearchStrategy primaryFirstSearchStrategy = new PrimaryFirstSearchStrategy();
    protected final Map<String, AbstractSearchStrategy> strategies = new ConcurrentHashMap<String, AbstractSearchStrategy>();
    protected final Map<String, AbstractSearchStrategy.Router> routers = new ConcurrentHashMap<String, AbstractSearchStrategy.Router>();

    private final ConsistencyLevel metadataWriteCL = consistencyLevelFromString(System.getProperty("elassandra.metadata.write.cl","QUORUM"));
    private final ConsistencyLevel metadataReadCL = consistencyLevelFromString(System.getProperty("elassandra.metadata.read.cl","QUORUM"));
    private final ConsistencyLevel metadataSerialCL = consistencyLevelFromString(System.getProperty("elassandra.metadata.serial.cl","SERIAL"));

    private final String elasticAdminKeyspaceName;
    private final String selectMetadataQuery;
    private final String selectVersionMetadataQuery;
    private final String insertMetadataQuery;
    private final String updateMetaDataQuery;

    private final SchemaManager schemaManager;
    private final QueryManager queryManager;

    @Inject
    public ClusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool,
            Map<String, java.util.function.Supplier<ClusterState.Custom>> initialClusterStateCustoms) {
        super(settings, clusterSettings, threadPool, initialClusterStateCustoms);
        this.operationRouting = new OperationRouting(settings, clusterSettings, this);
        this.mappingUpdatedAction = null;
        this.tokenRangeService = new TokenRangesService(settings);
        this.cassandraSecondaryIndicesApplier = new CassandraSecondaryIndicesApplier(settings, this);
        this.getClusterApplierService().setClusterService(this);
        this.getMasterService().setClusterService(this);
        this.schemaManager = new SchemaManager(settings, this);
        this.queryManager = new QueryManager(settings, this);

        String datacenterGroup = settings.get(SETTING_CLUSTER_DATACENTER_GROUP);
        if (datacenterGroup != null && datacenterGroup.length() > 0) {
            logger.info("Starting with datacenter.group=[{}]", datacenterGroup.trim().toLowerCase(Locale.ROOT));
            elasticAdminKeyspaceName = String.format(Locale.ROOT, "%s_%s", ELASTIC_ADMIN_KEYSPACE,datacenterGroup.trim().toLowerCase(Locale.ROOT));
        } else {
            elasticAdminKeyspaceName = ELASTIC_ADMIN_KEYSPACE;
        }
        selectMetadataQuery = String.format(Locale.ROOT, "SELECT metadata,version,owner FROM \"%s\".\"%s\" WHERE cluster_name = ?", elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE);
        selectVersionMetadataQuery = String.format(Locale.ROOT, "SELECT version FROM \"%s\".\"%s\" WHERE cluster_name = ?", elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE);
        insertMetadataQuery = String.format(Locale.ROOT, "INSERT INTO \"%s\".\"%s\" (cluster_name,owner,version,metadata) VALUES (?,?,?,?) IF NOT EXISTS", elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE);
        updateMetaDataQuery = String.format(Locale.ROOT, "UPDATE \"%s\".\"%s\" SET owner = ?, version = ?, metadata = ? WHERE cluster_name = ? IF version < ?", elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE);
    }

    @Override
    public OperationRouting operationRouting() {
        return operationRouting;
    }

    public void setMetaStateService(MetaStateService metaStateService) {
        this.metaStateService = metaStateService;
    }

    public void setIndicesService(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    public IndicesService getIndicesService() {
        return this.indicesService;
    }

    public QueryManager getQueryManager() {
        return this.queryManager;
    }

    public SchemaManager getSchemaManager() {
        return this.schemaManager;
    }

    public void setDiscovery(Discovery discovery) {
        this.discovery = (CassandraDiscovery)discovery;
    }

    public CassandraDiscovery getCassandraDiscovery() {
        return this.discovery;
    }

    public TokenRangesService tokenRangesService() {
        return this.tokenRangeService;
    }

    public void addShardStartedBarrier() {
        getMasterService().addShardStartedBarrier(new CassandraShardStartedBarrier(settings, this));
    }

    public void removeShardStartedBarrier() {
        getMasterService().removeShardStartedBarrier();
    }

    public void blockUntilShardsStarted() {
        getMasterService().blockUntilShardsStarted();
    }

    public String getElasticAdminKeyspaceName() {
        return this.elasticAdminKeyspaceName;
    }

    public Class<? extends AbstractSearchStrategy> searchStrategyClass(IndexMetaData indexMetaData, ClusterState state) {
        try {
            return AbstractSearchStrategy.getSearchStrategyClass(
                    indexMetaData.getSettings().get(IndexMetaData.SETTING_SEARCH_STRATEGY_CLASS,
                    state.metaData().settings().get(ClusterService.SETTING_CLUSTER_SEARCH_STRATEGY_CLASS,PrimaryFirstSearchStrategy.class.getName()))
                    );
        } catch(ConfigurationException e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("Bad search strategy class, fallback to [{}]", PrimaryFirstSearchStrategy.class.getName()), e);
            return PrimaryFirstSearchStrategy.class;
        }
    }

    private AbstractSearchStrategy searchStrategyInstance(Class<? extends AbstractSearchStrategy> clazz) {
        AbstractSearchStrategy searchStrategy = strategies.get(clazz.getName());
        if (searchStrategy == null) {
            try {
                searchStrategy = clazz.newInstance();
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("Cannot instanciate search strategy [{}]", clazz.getName()), e);
                searchStrategy = new PrimaryFirstSearchStrategy();
            }
            strategies.putIfAbsent(clazz.getName(), searchStrategy);
        }
        return searchStrategy;
    }


    public PrimaryFirstSearchStrategy.PrimaryFirstRouter updateRouter(IndexMetaData indexMetaData, ClusterState state) {
        // update and returns a PrimaryFirstRouter for the build table.
        PrimaryFirstSearchStrategy.PrimaryFirstRouter router = (PrimaryFirstSearchStrategy.PrimaryFirstRouter)this.primaryFirstSearchStrategy.newRouter(indexMetaData.getIndex(), indexMetaData.keyspace(), this::getShardRoutingStates, state);

        // update the router cache with the effective router
        AbstractSearchStrategy effectiveSearchStrategy = searchStrategyInstance(searchStrategyClass(indexMetaData, state));
        if (! effectiveSearchStrategy.equals(PrimaryFirstSearchStrategy.class) ) {
            AbstractSearchStrategy.Router router2 = effectiveSearchStrategy.newRouter(indexMetaData.getIndex(), indexMetaData.keyspace(), this::getShardRoutingStates, state);
            this.routers.put(indexMetaData.getIndex().getName(), router2);
        } else {
            this.routers.put(indexMetaData.getIndex().getName(), router);
        }

        return router;
    }

    public AbstractSearchStrategy.Router getRouter(IndexMetaData indexMetaData, ClusterState state) {
        AbstractSearchStrategy.Router router = this.routers.get(indexMetaData.getIndex().getName());
        return router;
    }

    public UntypedResultSet process(final ConsistencyLevel cl, final String query)
            throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return process(cl, null, query, new Long(0), new Object[] {});
    }

    public UntypedResultSet process(final ConsistencyLevel cl, ClientState clientState, final String query)
            throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return process(cl, null, clientState, query, new Long(0), new Object[] {});
    }

    public UntypedResultSet process(final ConsistencyLevel cl, ClientState clientState, final String query, Object... values)
            throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return process(cl, null, clientState, query, new Long(0), values);
    }

    public UntypedResultSet process(final ConsistencyLevel cl, final String query, Object... values)
            throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return process(cl, null, query, new Long(0), values);
    }

    public UntypedResultSet process(final ConsistencyLevel cl, final ConsistencyLevel serialConsistencyLevel, final String query, final Object... values)
            throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        return process(cl, serialConsistencyLevel, query, new Long(0), values);
    }

    public UntypedResultSet process(final ConsistencyLevel cl, final ConsistencyLevel serialConsistencyLevel, final String query, Long writetime, final Object... values) {
        return process(cl, serialConsistencyLevel, ClientState.forInternalCalls(), query, writetime, values);
    }

    public UntypedResultSet process(final ConsistencyLevel cl, final ConsistencyLevel serialConsistencyLevel, ClientState clientState, final String query, Long writetime, final Object... values)
            throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        if (logger.isDebugEnabled())
            logger.debug("processing CL={} SERIAL_CL={} query={}", cl, serialConsistencyLevel, query);

        // retreive prepared
        QueryState queryState = new QueryState(clientState);
        ResultMessage.Prepared prepared = ClientState.getCQLQueryHandler().prepare(query, queryState, Collections.EMPTY_MAP);

        // bind
        List<ByteBuffer> boundValues = new ArrayList<ByteBuffer>(values.length);
        for (int i = 0; i < values.length; i++) {
            Object v = values[i];
            AbstractType type = prepared.metadata.names.get(i).type;
            boundValues.add(v instanceof ByteBuffer || v == null ? (ByteBuffer) v : type.decompose(v));
        }

        // execute
        QueryOptions queryOptions = (serialConsistencyLevel == null) ? QueryOptions.forInternalCalls(cl, boundValues) : QueryOptions.forInternalCalls(cl, serialConsistencyLevel, boundValues);
        ResultMessage result = ClientState.getCQLQueryHandler().process(query, queryState, queryOptions, Collections.EMPTY_MAP, System.nanoTime());
        writetime = queryState.getTimestamp();
        return (result instanceof ResultMessage.Rows) ? UntypedResultSet.create(((ResultMessage.Rows) result).result) : null;
    }

    public boolean processWriteConditional(final ConsistencyLevel cl, final ConsistencyLevel serialCl, final String query, Object... values) {
        return processWriteConditional(cl, serialCl, ClientState.forInternalCalls(), query, values);
    }

    public boolean processWriteConditional(final ConsistencyLevel cl, final ConsistencyLevel serialCl, ClientState clientState, final String query, Object... values)
            throws RequestExecutionException, RequestValidationException, InvalidRequestException {
        try {
            UntypedResultSet result = process(cl, serialCl, clientState, query, new Long(0), values);
            if (serialCl == null)
                return true;

             if (!result.isEmpty()) {
                Row row = result.one();
                if (row.has("[applied]")) {
                     return row.getBoolean("[applied]");
                }
            }
            return false;
        } catch (WriteTimeoutException e) {
            logger.warn("PAXOS phase failed query=" + query + " values=" + Arrays.toString(values), e);
            return false;
        } catch (UnavailableException e) {
            logger.warn("PAXOS commit failed query=" + query + " values=" + Arrays.toString(values), e);
            return false;
        } catch (Exception e) {
            logger.error("Failed to process query=" + query + " values=" + Arrays.toString(values), e);
            throw e;
        }
    }

    public static String typeToCfName(String keyspaceName, String typeName) {
        return SchemaManager.typeToCfName(keyspaceName, typeName);
    }

    public static String buildIndexName(final String cfName) {
        return SchemaManager.buildIndexName(cfName);
    }

    public static String indexToKsName(String index) {
        return index.replaceAll("\\.", "_").replaceAll("\\-", "_");
    }

    public static int replicationFactor(String keyspace) {
        if (Schema.instance != null && Schema.instance.getKeyspaceInstance(keyspace) != null) {
            AbstractReplicationStrategy replicationStrategy = Schema.instance.getKeyspaceInstance(keyspace).getReplicationStrategy();
            int rf = replicationStrategy.getReplicationFactor();
            if (replicationStrategy instanceof NetworkTopologyStrategy) {
                rf = ((NetworkTopologyStrategy)replicationStrategy).getReplicationFactor(DatabaseDescriptor.getLocalDataCenter());
            }
            return rf;
        }
        return 0;
    }

    public ClusterState updateNumberOfReplica(ClusterState currentState) {
        MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());
        for(Iterator<IndexMetaData> it = currentState.metaData().iterator(); it.hasNext(); ) {
            IndexMetaData indexMetaData = it.next();
            IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData);
            int rf = replicationFactor(indexMetaData.keyspace());
            indexMetaDataBuilder.numberOfReplicas( Math.max(0, rf - 1) );
            metaDataBuilder.put(indexMetaDataBuilder.build(), false);
        }
        return ClusterState.builder(currentState).metaData(metaDataBuilder.build()).build();
    }

    public ClusterState updateNumberOfShardsAndReplicas(ClusterState currentState) {
        int numberOfNodes = currentState.nodes().getSize();

        if (numberOfNodes == 0)
            return currentState; // for testing purposes.

        MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());
        for(Iterator<IndexMetaData> it = currentState.metaData().iterator(); it.hasNext(); ) {
            IndexMetaData indexMetaData = it.next();
            IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData);
            indexMetaDataBuilder.numberOfShards(numberOfNodes);
            int rf = replicationFactor(indexMetaData.keyspace());
            indexMetaDataBuilder.numberOfReplicas( Math.max(0, rf - 1) );
            metaDataBuilder.put(indexMetaDataBuilder.build(), false);
        }
        return ClusterState.builder(currentState).metaData(metaDataBuilder.build()).build();
    }

    /**
     * Reload cluster metadata from elastic_admin.metadatatable row.
     * Should only be called when user keyspaces are initialized.
     */
    public void submitRefreshMetaData(final MetaData metaData, final String source) {
        submitStateUpdateTask(source, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return ClusterState.builder(currentState).metaData(metaData).build();
            }

            @Override
            public void onFailure(String source, Exception t) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure during [{}]", source), t);
            }
        });

    }

    public void submitNumberOfShardsAndReplicasUpdate(final String source) {
        submitStateUpdateTask(source, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updateNumberOfShardsAndReplicas(currentState);
            }

            @Override
            public void onFailure(String source, Exception t) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure during [{}]", source), t);
            }
        });
    }

    public void updateRoutingTable() {
        submitStateUpdateTask("Update-routing-table" , new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                 return ClusterState.builder(currentState).incrementVersion().build();
            }

            @Override
            public void onFailure(String source, Exception t) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure during [{}]", source), t);
            }
        });
    }

    @Override
    protected void doStart() {
        // add post-applied because 2i shoukd be created/deleted after that cassandra indices have taken the new mapping.
        this.addStateApplier(cassandraSecondaryIndicesApplier);

        super.doStart();

        // start a thread for asynchronous CQL schema update, always the last update.
        Runnable task = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try{
                        ClusterService.this.metadataToSaveSemaphore.acquire();
                        MetadataSchemaUpdate metadataSchemaUpdate = ClusterService.this.lastMetadataToSave.getAndSet(null);
                        if (metadataSchemaUpdate != null) {
                            if (metadataSchemaUpdate.version < state().metaData().version()) {
                                logger.trace("Giveup {}.{}.comment obsolete update of metadata.version={} timestamp={}",
                                        ELASTIC_ADMIN_KEYSPACE, ELASTIC_ADMIN_METADATA_TABLE,
                                        metadataSchemaUpdate.version, metadataSchemaUpdate.timestamp);
                            } else {
                                logger.trace("Applying {}.{}.comment update with metadata.version={} timestamp={}",
                                        ELASTIC_ADMIN_KEYSPACE, ELASTIC_ADMIN_METADATA_TABLE,
                                        metadataSchemaUpdate.version, metadataSchemaUpdate.timestamp);
                                // delayed CQL schema update with timestamp = time of cluster state update
                                CFMetaData cfm = SchemaManager.getCFMetaData(ELASTIC_ADMIN_KEYSPACE, ELASTIC_ADMIN_METADATA_TABLE).copy();
                                TableAttributes attrs = new TableAttributes();
                                attrs.addProperty(TableParams.Option.COMMENT.toString(), metadataSchemaUpdate.metaDataString);
                                cfm.params( attrs.asAlteredTableParams(cfm.params) );
                                MigrationManager.announceColumnFamilyUpdate(cfm, null, false, metadataSchemaUpdate.timestamp);
                            }
                        }
                    } catch(Exception e) {
                        logger.warn("Failed to update CQL schema",e);
                    }
                }
            }
        };
        new Thread(task, "metadataSchemaUpdater").start();
    }

    public void updateMapping(String ksName, MappingMetaData mapping) {
        cassandraSecondaryIndicesApplier.updateMapping( ksName, mapping);
    }

    public void recoverShard(String index) {
        cassandraSecondaryIndicesApplier.recoverShard(index);
    }

    public static class BlockingActionListener implements ActionListener<ClusterStateUpdateResponse> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile Throwable error = null;

        public void waitForUpdate(TimeValue timeValue) throws Exception {
            if (timeValue != null && timeValue.millis() > 0) {
                if (!latch.await(timeValue.millis(), TimeUnit.MILLISECONDS)) {
                    throw new ElasticsearchTimeoutException("blocking update timeout");
                }
            } else {
                latch.await();
            }
            if (error != null)
                throw new RuntimeException(error);
        }

        @Override
        public void onResponse(ClusterStateUpdateResponse response) {
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            error = e;
            latch.countDown();
        }
    }

    /**
     * CQL schema update must be asynchronous when triggered by a new dynamic field (see #91)
     * @param indexService
     * @param type
     * @param source
     * @throws Exception
     */
    public void blockingMappingUpdate(IndexService indexService, String type, String source) throws Exception {
        TimeValue timeout = settings.getAsTime(SETTING_CLUSTER_MAPPING_UPDATE_TIMEOUT, TimeValue.timeValueSeconds(Integer.getInteger(SETTING_SYSTEM_MAPPING_UPDATE_TIMEOUT, 30)));
        BlockingActionListener mappingUpdateListener = new BlockingActionListener();
        MetaDataMappingService metaDataMappingService = ElassandraDaemon.injector().getInstance(MetaDataMappingService.class);
        PutMappingClusterStateUpdateRequest putRequest = new PutMappingClusterStateUpdateRequest()
                .indices(new org.elasticsearch.index.Index[] {indexService.index()})
                .type(type)
                .source(source)
                .ackTimeout(timeout)
                .masterNodeTimeout(timeout);
        metaDataMappingService.putMapping(putRequest, mappingUpdateListener);
        mappingUpdateListener.waitForUpdate(timeout);
        logger.debug("Cluster state successfully updated for index=[{}], type=[{}], source=[{}] metadata.version={}/{}",
                indexService.index().getName(), type, source, state().metaData().clusterUUID(), state().metaData().version());
    }

    @SuppressForbidden(reason = "toUpperCase() for consistency level")
    public static ConsistencyLevel consistencyLevelFromString(String value) {
        switch(value.toUpperCase(Locale.ROOT)) {
        case "ANY": return ConsistencyLevel.ANY;
        case "ONE": return ConsistencyLevel.ONE;
        case "TWO": return ConsistencyLevel.TWO;
        case "THREE": return ConsistencyLevel.THREE;
        case "QUORUM": return ConsistencyLevel.QUORUM;
        case "ALL": return ConsistencyLevel.ALL;
        case "LOCAL_QUORUM": return ConsistencyLevel.LOCAL_QUORUM;
        case "EACH_QUORUM": return ConsistencyLevel.EACH_QUORUM;
        case "SERIAL": return ConsistencyLevel.SERIAL;
        case "LOCAL_SERIAL": return ConsistencyLevel.LOCAL_SERIAL;
        case "LOCAL_ONE": return ConsistencyLevel.LOCAL_ONE;
        default :
            throw new IllegalArgumentException("No write consistency match [" + value + "]");
        }
    }

    public boolean isDatacenterGroupMember(InetAddress endpoint) {
        String endpointDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
        KeyspaceMetadata  elasticAdminMetadata = Schema.instance.getKSMetaData(this.elasticAdminKeyspaceName);
        if (elasticAdminMetadata != null) {
            ReplicationParams replicationParams = elasticAdminMetadata.params.replication;
            if (replicationParams.klass == NetworkTopologyStrategy.class && replicationParams.options.get(endpointDc) != null) {
                return true;
            }
        }
        return false;
    }

    public class MetadataSchemaUpdate {
        long version;
        long timestamp;
        String metaDataString;

        MetadataSchemaUpdate(String metaDataString, long version) {
            this.metaDataString = metaDataString;
            this.version = version;
            this.timestamp = FBUtilities.timestampMicros();
        }
    }

    public void writeMetaDataAsComment(MetaData metaData) throws ConfigurationException, IOException {
        writeMetaDataAsComment( MetaData.Builder.toXContent(metaData, MetaData.CASSANDRA_FORMAT_PARAMS), metaData.version());
    }

    public void writeMetaDataAsComment(String metaDataString, long version) throws ConfigurationException, IOException {
        // Issue #91, update C* schema asynchronously to avoid inter-locking with map column as nested object.
        logger.trace("Submit asynchronous CQL schema update for metadata={}", metaDataString);
        this.lastMetadataToSave.set(new MetadataSchemaUpdate(metaDataString, version));
        this.metadataToSaveSemaphore.release();
    }

    /**
     * Should only be used after a SCHEMA change.
     */

    public MetaData readMetaDataAsComment() throws NoPersistedMetaDataException {
        try {
            String query = String.format(Locale.ROOT, "SELECT comment FROM system_schema.tables WHERE keyspace_name='%s' AND table_name='%s'",
                this.elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE);
            UntypedResultSet result = QueryProcessor.executeInternal(query);
            if (result.isEmpty())
                throw new NoPersistedMetaDataException("Failed to read comment from "+elasticAdminKeyspaceName+"+"+ELASTIC_ADMIN_METADATA_TABLE);

            String metadataString = result.one().getString("comment");
            logger.debug("Recover metadata from {}.{} = {}", elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE, metadataString);
            return parseMetaDataString( metadataString );
        } catch (RequestValidationException | RequestExecutionException e) {
            throw new NoPersistedMetaDataException("Failed to read comment from "+elasticAdminKeyspaceName+"+"+ELASTIC_ADMIN_METADATA_TABLE, e);
        }
    }

    private MetaData parseMetaDataString(String metadataString) throws NoPersistedMetaDataException {
        if (metadataString != null && metadataString.length() > 0) {
            MetaData metaData;
            try {
                metaData =  metaStateService.loadGlobalState(metadataString);

                // initialize typeToCfName map for later reverse lookup in ElasticSecondaryIndex
                for(ObjectCursor<IndexMetaData> indexCursor : metaData.indices().values()) {
                    for(ObjectCursor<MappingMetaData> mappingCursor :  indexCursor.value.getMappings().values()) {
                        String cfName = SchemaManager.typeToCfName(indexCursor.value.keyspace(), mappingCursor.value.type());
                        if (logger.isDebugEnabled())
                            logger.debug("keyspace.table={}.{} registred for elasticsearch index.type={}.{}",
		           indexCursor.value.keyspace(), cfName, indexCursor.value.getIndex().getName(), mappingCursor.value.type());
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to parse metadata={}", e, metadataString);
                throw new NoPersistedMetaDataException("Failed to parse metadata="+metadataString, e);
            }
            return metaData;
        }
        throw new NoPersistedMetaDataException("metadata null or empty");
    }

    /**
     * Try to read fresher metadata from cassandra.
     */
    public MetaData checkForNewMetaData(Long expectedVersion) throws NoPersistedMetaDataException {
        MetaData quorumMetaData = readMetaDataAsRow(this.metadataReadCL);
        return quorumMetaData;
    }


    public MetaData readInternalMetaDataAsRow() throws NoPersistedMetaDataException {
        try {
            UntypedResultSet rs = QueryProcessor.executeInternal(selectMetadataQuery, DatabaseDescriptor.getClusterName());
            if (rs != null && !rs.isEmpty()) {
                Row row = rs.one();
                if (row.has("metadata"))
                    return parseMetaDataString(row.getString("metadata"));
            }
        } catch (Exception e) {
            logger.warn("Cannot read metadata locally",e);
        }
        return null;
    }

    public MetaData readMetaDataAsRow(ConsistencyLevel cl) throws NoPersistedMetaDataException {
        try {
            UntypedResultSet rs = process(cl, ClientState.forInternalCalls(), selectMetadataQuery, DatabaseDescriptor.getClusterName());
            if (rs != null && !rs.isEmpty()) {
                Row row = rs.one();
                if (row.has("metadata"))
                    return parseMetaDataString(row.getString("metadata"));
            }
        } catch (UnavailableException e) {
            logger.warn("Cannot read elasticsearch metadata with consistency="+cl, e);
            return null;
        } catch (KeyspaceNotDefinedException e) {
            logger.warn("Keyspace {} not yet defined", elasticAdminKeyspaceName);
            return null;
        } catch (Exception e) {
            throw new NoPersistedMetaDataException("Unexpected error",e);
        }
        throw new NoPersistedMetaDataException("No elasticsearch metadata available");
    }

    public Long readMetaDataVersion(ConsistencyLevel cl) throws NoPersistedMetaDataException {
        try {
            UntypedResultSet rs = process(cl, ClientState.forInternalCalls(), selectVersionMetadataQuery, DatabaseDescriptor.getClusterName());
            if (rs != null && !rs.isEmpty()) {
                Row row = rs.one();
                if (row.has("version"))
                    return row.getLong("version");
            }
        } catch (Exception e) {
            logger.warn("unexpected error", e);
        }
        return -1L;
    }
    public static String getElasticsearchClusterName(Settings settings) {
        String clusterName = DatabaseDescriptor.getClusterName();
        String datacenterGroup = settings.get(ClusterService.SETTING_CLUSTER_DATACENTER_GROUP);
        if (datacenterGroup != null) {
            clusterName = DatabaseDescriptor.getClusterName() + "@" + datacenterGroup.trim();
        }
        return clusterName;
    }

    public int getLocalDataCenterSize() {
        int count = 1;
        for (UntypedResultSet.Row row : executeInternal("SELECT data_center, rpc_address FROM system." + SystemKeyspace.PEERS))
            if (row.has("rpc_address") && row.has("data_center") && DatabaseDescriptor.getLocalDataCenter().equals(row.getString("data_center")))
                count++;
        logger.info(" datacenter=[{}] size={} from peers", DatabaseDescriptor.getLocalDataCenter(), count);
        return count;
    }


    Void createElasticAdminKeyspace()  {
        try {
            Map<String, String> replication = new HashMap<String, String>();

            replication.put("class", NetworkTopologyStrategy.class.getName());
            replication.put(DatabaseDescriptor.getLocalDataCenter(), Integer.toString(getLocalDataCenterSize()));

            String createKeyspace = String.format(Locale.ROOT, "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH replication = %s;",
                elasticAdminKeyspaceName, FBUtilities.json(replication).replaceAll("\"", "'"));
            logger.info(createKeyspace);
            process(ConsistencyLevel.LOCAL_ONE, ClientState.forInternalCalls(), createKeyspace);
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("Failed to initialize keyspace {}", elasticAdminKeyspaceName), e);
            throw e;
        }
        return null;
    }


    // Modify keyspace replication
    public void alterKeyspaceReplicationFactor(String keyspaceName, int rf) {
        ReplicationParams replication = Schema.instance.getKSMetaData(keyspaceName).params.replication;

        if (!NetworkTopologyStrategy.class.getName().equals(replication.klass))
            throw new ConfigurationException("Keyspace ["+keyspaceName+"] should use "+NetworkTopologyStrategy.class.getName()+" replication strategy");

        Map<String, String> repMap = replication.asMap();

        if (!repMap.containsKey(DatabaseDescriptor.getLocalDataCenter()) || !Integer.toString(rf).equals(repMap.get(DatabaseDescriptor.getLocalDataCenter()))) {
            repMap.put(DatabaseDescriptor.getLocalDataCenter(), Integer.toString(rf));
            logger.debug("Updating keyspace={} replication={}", keyspaceName, repMap);
            try {
                String query = String.format(Locale.ROOT, "ALTER KEYSPACE \"%s\" WITH replication = %s",
                        keyspaceName, FBUtilities.json(repMap).replaceAll("\"", "'"));
                logger.info(query);
                process(ConsistencyLevel.LOCAL_ONE, ClientState.forInternalCalls(), query);
            } catch (Throwable e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("Failed to alter keyspace [{}]",keyspaceName), e);
                throw e;
            }
        } else {
            logger.info("Keep unchanged keyspace={} datacenter={} RF={}", keyspaceName, DatabaseDescriptor.getLocalDataCenter(), rf);
        }
    }

    // Create The meta Data Table if needed
    Void createElasticAdminMetaTable(final String metaDataString) {
        try {
            String createTable = String.format(Locale.ROOT, "CREATE TABLE IF NOT EXISTS \"%s\".%s ( cluster_name text PRIMARY KEY, owner uuid, version bigint, metadata text) WITH comment='%s';",
                elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE, metaDataString);
            logger.info(createTable);
            process(ConsistencyLevel.LOCAL_ONE, ClientState.forInternalCalls(), createTable);
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("Failed to initialize table {}.{}", elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE), e);
            throw e;
        }
        return null;
    }

    // initialize a first row if needed
    Void insertFirstMetaRow(final MetaData metadata, final String metaDataString) {
        try {
            logger.info(insertMetadataQuery);
            process(ConsistencyLevel.LOCAL_ONE, ClientState.forInternalCalls(), insertMetadataQuery,
                DatabaseDescriptor.getClusterName(), UUID.fromString(StorageService.instance.getLocalHostId()), metadata.version(), metaDataString);
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("Failed insert first row into table {}.{}", elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE), e);
            throw e;
        }
        return null;
    }

    void retry (final Supplier<Void> function, final String label) {
        for (int i = 0; ; ++i) {
            try {
                function.get();
                break;
            } catch (final Exception e) {
                if (i >= CREATE_ELASTIC_ADMIN_RETRY_ATTEMPTS) {
                    logger.error("Failed to {} after {} attempts", label, CREATE_ELASTIC_ADMIN_RETRY_ATTEMPTS);
                    throw new NoPersistedMetaDataException("Failed to " + label + " after " + CREATE_ELASTIC_ADMIN_RETRY_ATTEMPTS + " attempts", e);
                } else
                    logger.info("Retrying: {}", label);
            }
        }
    }

    /**
     * Create or update elastic_admin keyspace.
     */

    public void createOrUpdateElasticAdminKeyspace()  {
        UntypedResultSet result = QueryProcessor.executeOnceInternal(String.format(Locale.ROOT, "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name='%s'", elasticAdminKeyspaceName));
        if (result.isEmpty()) {
            MetaData metadata = state().metaData();
            try {
                final String metaDataString;
                try {
                    metaDataString = MetaData.Builder.toXContent(metadata);
                } catch (IOException e) {
                    logger.error("Failed to build metadata", e);
                    throw new NoPersistedMetaDataException("Failed to build metadata", e);
                }
                // create elastic_admin if not exists after joining the ring and before allowing metadata update.
                retry(() -> createElasticAdminKeyspace(), "create elastic admin keyspace");
                retry(() -> createElasticAdminMetaTable(metaDataString), "create elastic admin metadata table");
                retry(() -> insertFirstMetaRow(metadata, metaDataString), "write first row to metadata table");
                logger.info("Succefully initialize {}.{} = {}", elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE, metaDataString);
                try {
                    writeMetaDataAsComment(metaDataString, metadata.version());
                } catch (IOException e) {
                    logger.error("Failed to write metadata as comment", e);
                }
            } catch (Throwable e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("Failed to initialize table {}.{}", elasticAdminKeyspaceName, ELASTIC_ADMIN_METADATA_TABLE),e);
            }
        } else {
            Map<String,String> replication = result.one().getFrozenTextMap("replication");
            logger.debug("keyspace={} replication={}", elasticAdminKeyspaceName, replication);

            if (!NetworkTopologyStrategy.class.getName().equals(replication.get("class")))
                    throw new ConfigurationException("Keyspace ["+this.elasticAdminKeyspaceName+"] should use "+NetworkTopologyStrategy.class.getName()+" replication strategy");

            int currentRF = -1;
            if (replication.get(DatabaseDescriptor.getLocalDataCenter()) != null) {
                currentRF = Integer.valueOf(replication.get(DatabaseDescriptor.getLocalDataCenter()).toString());
            }
            int targetRF = getLocalDataCenterSize();
            if (targetRF != currentRF) {
                replication.put(DatabaseDescriptor.getLocalDataCenter(), Integer.toString(targetRF));
                try {
                    String query = String.format(Locale.ROOT, "ALTER KEYSPACE \"%s\" WITH replication = %s",
                            elasticAdminKeyspaceName, FBUtilities.json(replication).replaceAll("\"", "'"));
                    logger.info(query);
                    process(ConsistencyLevel.LOCAL_ONE, ClientState.forInternalCalls(), query);
                } catch (Throwable e) {
                    logger.error((Supplier<?>) () -> new ParameterizedMessage("Failed to alter keyspace [{}]", elasticAdminKeyspaceName),e);
                    throw e;
                }
            } else {
                logger.info("Keep unchanged keyspace={} datacenter={} RF={}", elasticAdminKeyspaceName, DatabaseDescriptor.getLocalDataCenter(), targetRF);
            }
        }
    }


    public ShardInfo shardInfo(String index, ConsistencyLevel cl) {
        Keyspace keyspace = Schema.instance.getKeyspaceInstance(state().metaData().index(index).keyspace());
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        int rf = replicationStrategy.getReplicationFactor();
        if (replicationStrategy instanceof NetworkTopologyStrategy)
            rf = ((NetworkTopologyStrategy)replicationStrategy).getReplicationFactor(DatabaseDescriptor.getLocalDataCenter());
        return new ShardInfo(rf, cl.blockFor(keyspace));
    }


    public void persistMetaData(MetaData oldMetaData, MetaData newMetaData, String source) throws IOException, InvalidRequestException, RequestExecutionException, RequestValidationException {
        if (!newMetaData.clusterUUID().equals(localNode().getId())) {
            logger.error("should not push metadata updated from another node {}/{}", newMetaData.clusterUUID(), newMetaData.version());
            return;
        }
        if (newMetaData.clusterUUID().equals(state().metaData().clusterUUID()) && newMetaData.version() < state().metaData().version()) {
            logger.warn("don't push obsolete metadata uuid={} version {} < {}", newMetaData.clusterUUID(), newMetaData.version(), state().metaData().version());
            return;
        }

        String metaDataString = MetaData.Builder.toXContent(newMetaData, MetaData.CASSANDRA_FORMAT_PARAMS);
        UUID owner = UUID.fromString(localNode().getId());
        boolean applied = processWriteConditional(
                this.metadataWriteCL,
                this.metadataSerialCL,
                ClientState.forInternalCalls(),
                updateMetaDataQuery,
                new Object[] { owner, newMetaData.version(), metaDataString, DatabaseDescriptor.getClusterName(), newMetaData.version() });
        if (applied) {
            logger.debug("PAXOS Succefully update metadata source={} newMetaData={} in cluster {}", source, metaDataString, DatabaseDescriptor.getClusterName());
            writeMetaDataAsComment(metaDataString, newMetaData.version());
            return;
        } else {
            logger.warn("PAXOS Failed to update metadata oldMetadata={}/{} currentMetaData={}/{} in cluster {}",
                    oldMetaData.clusterUUID(), oldMetaData.version(), localNode().getId(), newMetaData.version(), DatabaseDescriptor.getClusterName());
            throw new ConcurrentMetaDataUpdateException(owner, newMetaData.version());
        }
    }

    /**
     * Duplicate code from org.apache.cassandra.service.StorageService.setLoggingLevel, allowing to set log level without StorageService.instance for tests.
     * @param classQualifier
     * @param rawLevel
     */
    public static void setLoggingLevel(String classQualifier, String rawLevel)
    {
        ch.qos.logback.classic.Logger logBackLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(classQualifier);

        // if both classQualifer and rawLevel are empty, reload from configuration
        if (StringUtils.isBlank(classQualifier) && StringUtils.isBlank(rawLevel) )
        {
            try {
                JMXConfiguratorMBean jmxConfiguratorMBean = JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(),
                        new ObjectName("ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator"),
                        JMXConfiguratorMBean.class);
                jmxConfiguratorMBean.reloadDefaultConfiguration();
                return;
            } catch (MalformedObjectNameException | JoranException e) {
                throw new RuntimeException(e);
            }
        }
        // classQualifer is set, but blank level given
        else if (StringUtils.isNotBlank(classQualifier) && StringUtils.isBlank(rawLevel) )
        {
            if (logBackLogger.getLevel() != null || hasAppenders(logBackLogger))
                logBackLogger.setLevel(null);
            return;
        }

        ch.qos.logback.classic.Level level = ch.qos.logback.classic.Level.toLevel(rawLevel);
        logBackLogger.setLevel(level);
        //logger.info("set log level to {} for classes under '{}' (if the level doesn't look like '{}' then the logger couldn't parse '{}')", level, classQualifier, rawLevel, rawLevel);
    }

    private static boolean hasAppenders(ch.qos.logback.classic.Logger logger)
    {
        Iterator<Appender<ILoggingEvent>> it = logger.iteratorForAppenders();
        return it.hasNext();
    }

    public IndexService indexService(org.elasticsearch.index.Index index) {
        return this.indicesService.indexService(index);
    }

    public IndexService indexServiceSafe(org.elasticsearch.index.Index index) {
        return this.indicesService.indexServiceSafe(index);
    }

    /**
     * Return a set of started shards according t the gossip state map and the local shard state.
     */
    public ShardRoutingState getShardRoutingStates(org.elasticsearch.index.Index index, UUID nodeUuid) {
        if (nodeUuid.equals(this.localNode().uuid())) {
            if (this.discovery.isSearchEnabled()) {
                try {
                    IndexShard localIndexShard = indexServiceSafe(index).getShardOrNull(0);
                    if (localIndexShard != null && localIndexShard.routingEntry() != null)
                        return localIndexShard.routingEntry().state();
                } catch (IndexNotFoundException e) {
                }
            }
            return ShardRoutingState.UNASSIGNED;
        }

        // read-only map.
        Map<String, ShardRoutingState> shards = (this.discovery).getShardRoutingState(nodeUuid);
        if (shards == null) {
            if (logger.isDebugEnabled() && state().nodes().get(nodeUuid.toString()).status().equals(DiscoveryNodeStatus.ALIVE))
                logger.debug("No ShardRoutingState for alive node=[{}]",nodeUuid.toString());
            return ShardRoutingState.UNASSIGNED;
        }
        return shards.get(index.getName());
    }

    /**
     * Set index shard state in the gossip endpoint map (must be synchronized).
     */
    public void publishShardRoutingState(final String index, final ShardRoutingState shardRoutingState) throws JsonGenerationException, JsonMappingException, IOException {
        if (this.discovery != null)
            this.discovery.publishShardRoutingState(index, shardRoutingState);
    }

    /**
     * Publish cluster metadata uuid and version in gossip state.
     */
    public void publishX2(final ClusterState clusterState) {
        if (this.discovery != null)
            this.discovery.publishX2(clusterState);
    }

    @Override
    public DiscoveryNode localNode() {
        return (this.discovery != null) ? this.discovery.localNode() : this.getClusterApplierService().state().nodes().getLocalNode();
    }
}
