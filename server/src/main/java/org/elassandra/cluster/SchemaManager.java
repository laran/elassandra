/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elassandra.cluster;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ColumnDefinition.Kind;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.elassandra.index.mapper.internal.TokenFieldMapper;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.Mapper.CqlCollection;
import org.elasticsearch.index.mapper.Mapper.CqlStruct;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;

public class SchemaManager extends AbstractComponent {
    ClusterService clusterService;

    public static final String GEO_POINT_TYPE = "geo_point";
    public static final String ATTACHEMENT_TYPE = "attachement";
    public static final String COMPLETION_TYPE = "completion";

    public static final String ELASTIC_ID_COLUMN_NAME = "_id";

    public static Map<String, String> cqlMapping = new ImmutableMap.Builder<String,String>()
            .put("text", "keyword")
            .put("varchar", "keyword")
            .put("timestamp", "date")
            .put("date", "date")
            .put("time", "long")
            .put("smallint", "short")
            .put("tinyint", "byte")
            .put("int", "integer")
            .put("bigint", "long")
            .put("double", "double")
            .put("float", "float")
            .put("boolean", "boolean")
            .put("blob", "binary")
            .put("inet", "ip" )
            .put("uuid", "keyword" )
            .put("timeuuid", "keyword" )
            .build();

    public SchemaManager(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
    }

    public boolean isNativeCql3Type(String cqlType) {
        return cqlMapping.keySet().contains(cqlType) && !cqlType.startsWith("geo_");
    }

    public static CFMetaData getCFMetaData(final String ksName, final String cfName) throws ActionRequestValidationException {
        CFMetaData metadata = Schema.instance.getCFMetaData(ksName, cfName);
        if (metadata == null) {
            ActionRequestValidationException arve = new ActionRequestValidationException();
            arve.addValidationError(ksName+"."+cfName+" table does not exists");
            throw arve;
        }
        return metadata;
    }

    public static KeyspaceMetadata getKSMetaData(final String ksName) throws ActionRequestValidationException {
        KeyspaceMetadata metadata = Schema.instance.getKSMetaData(ksName);
        if (metadata == null) {
            ActionRequestValidationException arve = new ActionRequestValidationException();
            arve.addValidationError("Keyspace " + ksName + " does not exists");
            throw arve;
        }
        return metadata;
    }

    private void buildType(final String ksName, final String typeName, final String statement, final Set<String> updatedUserTypes) throws RequestExecutionException
    {
        if (getUDTInfo(ksName, typeName) == null) {
            QueryProcessor.executeInternal(statement);
            updatedUserTypes.add(typeName);
        }
    }

    private void buildGeoPointType(String ksName, Set<String> updatedUserTypes) throws RequestExecutionException {
        buildType(ksName,
                GEO_POINT_TYPE,
                String.format(Locale.ROOT, "CREATE TYPE IF NOT EXISTS \"%s\".\"%s\" ( %s double, %s double)",
                        ksName, GEO_POINT_TYPE,org.elasticsearch.common.geo.GeoUtils.LATITUDE, org.elasticsearch.common.geo.GeoUtils.LONGITUDE),
                updatedUserTypes);
    }

    private void buildAttachementType(String ksName, Set<String> updatedUserTypes) throws RequestExecutionException {
        buildType(ksName,
                ATTACHEMENT_TYPE,
                String.format(Locale.ROOT, "CREATE TYPE IF NOT EXISTS \"%s\".\"%s\" (context text, content_type text, content_length bigint, date timestamp, title text, author text, keywords text, language text)", ksName, ATTACHEMENT_TYPE),
                updatedUserTypes);
    }

    private void buildCompletionType(String ksName, Set<String> updatedUserTypes) throws RequestExecutionException {
        buildType(ksName,
                ATTACHEMENT_TYPE,
                String.format(Locale.ROOT, "CREATE TYPE IF NOT EXISTS \"%s\".\"%s\" (input list<text>, contexts text, weight int)", ksName, COMPLETION_TYPE),
                updatedUserTypes);
    }

    public static final String PERCOLATOR_TABLE = "_percolator";

    // Because Cassandra table name does not support dash, convert dash to underscore in elasticsearch type, an keep this information
    // in a map for reverse lookup. Of course, conflict is still possible in a keyspace.
    private static final Map<String, String> cfNameToType = new ConcurrentHashMap<String, String>() {{
       put(PERCOLATOR_TABLE, "percolator");
    }};

    public static String typeToCfName(String keyspaceName, String typeName) {
        if (typeName.indexOf('-') >= 0) {
            String cfName = typeName.replaceAll("\\-", "_");
            cfNameToType.putIfAbsent(keyspaceName+"."+cfName, typeName);
            return cfName;
        }
        return typeName;
    }

    public static String cfNameToType(String keyspaceName, String cfName) {
        if (cfName.indexOf('_') >= 0) {
            String type = cfNameToType.get(keyspaceName+"."+cfName);
            if (type != null)
                return type;
        }
        return cfName;
    }

    /**
     * Don't use QueryProcessor.executeInternal, we need to propagate this on all nodes.
     **/
    public boolean createIndexKeyspace(final String ksname, final int replicationFactor, final Map<String, Integer> replicationMap) throws IOException {
        Keyspace ks = null;
        try {
            ks = Keyspace.open(ksname);
            if (ks != null && !(ks.getReplicationStrategy() instanceof NetworkTopologyStrategy)) {
                throw new IOException("Cannot create index, underlying keyspace requires the NetworkTopologyStrategy.");
            }
        } catch(AssertionError | NullPointerException e) {
        }

        try {
            if (ks == null) {
                Map<String, String> replication = new HashMap<>();
                replication.put("class", "NetworkTopologyStrategy");
                replication.put(DatabaseDescriptor.getLocalDataCenter(), Integer.toString(replicationFactor));
                for(Map.Entry<String, Integer> entry : replicationMap.entrySet())
                    replication.put(entry.getKey(), Integer.toString(entry.getValue()));
                this.clusterService.process(ConsistencyLevel.LOCAL_ONE, ClientState.forInternalCalls(),
                    String.format(Locale.ROOT, "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH replication = %s",
                            ksname,  FBUtilities.json(replication).replaceAll("\"", "'")));
                return true;
            }
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
        return false;
    }


    public void dropIndexKeyspace(final String ksname) throws IOException {
        try {
            String query = String.format(Locale.ROOT, "DROP KEYSPACE IF EXISTS \"%s\"", ksname);
            logger.debug(query);
            QueryProcessor.process(query, ConsistencyLevel.LOCAL_ONE);
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    class ColumnDescriptor {
        String name;
        String type;
        ColumnDefinition.Kind kind;
        int position;

        public ColumnDescriptor(String name) {
            this(name, null);
        }

        public ColumnDescriptor(String name, String type) {
            this(name, type, Kind.REGULAR, -1);
        }

        public ColumnDescriptor(String name, String type, Kind kind, int position) {
            this.name = name;
            this.type = type;
            this.kind = kind;
            this.position = position;
        }
    }

    public void updateTableSchema(final MapperService mapperService, final MappingMetaData mappingMd, final boolean doApply) throws IOException {
        try {
            boolean announce = false;
            Set<String> updatedUserTypes = new HashSet<>();
            Set<ColumnIdentifier> addedColumns = new HashSet<>();
            long schemaUpdateTimestamp = System.currentTimeMillis();

            String ksName = mapperService.keyspace();
            String cfName = typeToCfName(ksName, mappingMd.type());

            announce = createIndexKeyspace(ksName, settings().getAsInt(SETTING_NUMBER_OF_REPLICAS, 0) +1, mapperService.getIndexSettings().getIndexMetaData().replication());

            final CFMetaData cfm = Schema.instance.getCFMetaData(ksName, cfName);
            boolean newTable = (cfm == null);

            DocumentMapper docMapper = mapperService.documentMapper(mappingMd.type());
            Map<String, Object> mappingMap = mappingMd.sourceAsMap();

            Set<String> columns = new HashSet();
            if (mapperService.getIndexSettings().getIndexMetaData().isOpaqueStorage()) {
                columns.add(SourceFieldMapper.NAME);
            } else {
                if (docMapper.sourceMapper().enabled())
                    columns.add(SourceFieldMapper.NAME);
                if (mappingMap.get("properties") != null)
                    columns.addAll(((Map<String, Object>) mappingMap.get("properties")).keySet());
            }

            logger.debug("Updating CQL3 schema {}.{} apply={} columns={}", ksName, cfName, doApply, columns);
            List<ColumnDescriptor> columnsList = new ArrayList<>();
            ColumnDescriptor[] primaryKeyList = new ColumnDescriptor[(newTable) ? columns.size()+1 : cfm.partitionKeyColumns().size()+cfm.clusteringColumns().size()];
            int primaryKeyLength = 0;
            int partitionKeyLength = 0;
            for (String column : columns) {
                if (isReservedKeyword(column))
                    logger.warn("Allowing a CQL reserved keyword in ES: {}", column);

                if (column.equals(TokenFieldMapper.NAME))
                    continue; // ignore pseudo column known by Elasticsearch

                ColumnDescriptor colDesc = new ColumnDescriptor(column);
                FieldMapper fieldMapper = docMapper.mappers().smartNameFieldMapper(column);
                if (fieldMapper != null) {
                    if (fieldMapper instanceof GeoPointFieldMapper) {
                        ColumnDefinition cdef = (newTable) ? null : cfm.getColumnDefinition(new ColumnIdentifier(column, true));
                        if (cdef != null && cdef.type instanceof UTF8Type) {
                            // index geohash stored as text in cassandra.
                            colDesc.type = "text";
                        } else {
                            // create a geo_point UDT to store lat,lon
                            colDesc.type = GEO_POINT_TYPE;
                            buildGeoPointType(ksName, updatedUserTypes);
                        }
                    } else if (fieldMapper instanceof GeoShapeFieldMapper) {
                        colDesc.type = "text";
                    } else if (fieldMapper instanceof CompletionFieldMapper) {
                        colDesc.type = COMPLETION_TYPE;
                        buildCompletionType(ksName, updatedUserTypes);
                    } else if (fieldMapper.getClass().getName().equals("org.elasticsearch.mapper.attachments.AttachmentMapper")) {
                        // attachement is a plugin, so class may not found.
                        colDesc.type = ATTACHEMENT_TYPE;
                        buildAttachementType(ksName, updatedUserTypes);
                    } else if (fieldMapper instanceof SourceFieldMapper) {
                        colDesc.type = "blob";
                    } else {
                        colDesc.type = fieldMapper.cqlType();
                        if (colDesc.type == null) {
                            logger.warn("Ignoring field [{}] type [{}]", column, fieldMapper.name());
                            continue;
                        }
                    }

                    if (fieldMapper.cqlPrimaryKeyOrder() >= 0) {
                        if (fieldMapper.cqlPrimaryKeyOrder() < primaryKeyList.length && primaryKeyList[fieldMapper.cqlPrimaryKeyOrder()] == null) {
                            primaryKeyList[fieldMapper.cqlPrimaryKeyOrder()] = colDesc;
                            colDesc.position = fieldMapper.cqlPrimaryKeyOrder();
                            primaryKeyLength = Math.max(primaryKeyLength, fieldMapper.cqlPrimaryKeyOrder()+1);
                            if (fieldMapper.cqlPartitionKey()) {
                                partitionKeyLength++;
                                colDesc.kind = Kind.PARTITION_KEY;
                            } else {
                                colDesc.kind = Kind.CLUSTERING;
                            }
                        } else {
                            throw new ConfigurationException("Wrong primary key order for column "+column);
                        }
                    }

                    if (!isNativeCql3Type(colDesc.type)) {
                        colDesc.type = "frozen<" + colDesc.type + ">";
                    }
                    if (!fieldMapper.cqlCollection().equals(CqlCollection.SINGLETON)) {
                        colDesc.type = fieldMapper.cqlCollectionTag()+"<" + colDesc.type + ">";
                    }
                    if (fieldMapper.cqlStaticColumn())
                        colDesc.kind = Kind.STATIC;
                } else {
                    ObjectMapper objectMapper = docMapper.objectMappers().get(column);
                    if (objectMapper == null) {
                       logger.warn("Cannot infer CQL type from object mapping for field [{}], ignoring", column);
                       continue;
                    }
                    if (objectMapper.isEnabled() && !objectMapper.iterator().hasNext()) {
                        logger.warn("Ignoring enabled object with no sub-fields", column);
                        continue;
                    }
                    if (objectMapper.cqlPrimaryKeyOrder() >= 0) {
                        if (objectMapper.cqlPrimaryKeyOrder() < primaryKeyList.length && primaryKeyList[objectMapper.cqlPrimaryKeyOrder()] == null) {
                            primaryKeyList[objectMapper.cqlPrimaryKeyOrder()] = colDesc;
                            colDesc.position = fieldMapper.cqlPrimaryKeyOrder();
                            primaryKeyLength = Math.max(primaryKeyLength, objectMapper.cqlPrimaryKeyOrder()+1);
                            if (objectMapper.cqlPartitionKey()) {
                                partitionKeyLength++;
                                colDesc.kind = Kind.PARTITION_KEY;
                            } else {
                                colDesc.kind = Kind.CLUSTERING;
                            }
                        } else {
                            throw new ConfigurationException("Wrong primary key order for column "+column);
                        }
                    }
                    if (!objectMapper.isEnabled()) {
                        logger.debug("Object [{}] not enabled stored as text", column);
                        colDesc.type = "text";
                    } else if (objectMapper.cqlStruct().equals(CqlStruct.MAP)) {
                        // TODO: check columnName exists and is map<text,?>
                        colDesc.type = buildCql(ksName, cfName, column, objectMapper, updatedUserTypes);
                        if (colDesc.type == null) {
                            // no sub-field, ignore it #146
                            continue;
                        }
                        if (!objectMapper.cqlCollection().equals(CqlCollection.SINGLETON)) {
                            colDesc.type = objectMapper.cqlCollectionTag()+"<"+colDesc.type+">";
                        }
                        //logger.debug("Expecting column [{}] to be a map<text,?>", column);
                    } else  if (objectMapper.cqlStruct().equals(CqlStruct.UDT)) {

                        if (!objectMapper.isEnabled()) {
                            colDesc.type = "text";   // opaque json object stored as text
                        } else {
                            String subType = buildCql(ksName, cfName, column, objectMapper, updatedUserTypes);
                            if (subType == null) {
                                continue;       // no sub-field, ignore it #146
                            }
                            colDesc.type = "frozen<" + ColumnIdentifier.maybeQuote(subType) + ">";
                        }
                        if (!objectMapper.cqlCollection().equals(CqlCollection.SINGLETON) && !(cfName.equals(PERCOLATOR_TABLE) && column.equals("query"))) {
                            colDesc.type = objectMapper.cqlCollectionTag()+"<"+colDesc.type+">";
                        }
                    }
                    if (objectMapper.cqlStaticColumn())
                        colDesc.kind = Kind.STATIC;
                }
                columnsList.add(colDesc);
            }

            // add _id if no PK
            if (partitionKeyLength == 0) {
                ColumnDescriptor colDesc = new ColumnDescriptor(ELASTIC_ID_COLUMN_NAME, "text", Kind.PARTITION_KEY, 0);
                columnsList.add(colDesc);
                primaryKeyList[0] = colDesc;
                primaryKeyLength = 1;
                partitionKeyLength = 1;
            }

            // add _parent column if necessary. Parent and child documents should have the same partition key.
            if (docMapper.parentFieldMapper().active() && docMapper.parentFieldMapper().pkColumns() == null) {
                columnsList.add(new ColumnDescriptor("_parent", "text"));
            }

            if (!doApply) {
                // check for CQL schema validity
                for(int i=0; i < primaryKeyLength; i++) {
                    if (primaryKeyList[i] == null)
                        throw new IOException("Bad mapping, primary key column not defined at position "+i);
                }
                if (!newTable) {
                    // check column properties matches existing ones
                    for(ColumnDescriptor colDesc : columnsList) {
                        ColumnDefinition cd = cfm.getColumnDefinition(new ColumnIdentifier(colDesc.name, true));
                        // do not enforce PK constraints if not specified explicitly in oder to keep backward compatibility.
                        if (cd != null && cd.kind != colDesc.kind && colDesc.kind != Kind.REGULAR)
                            throw new IOException("Bad mapping, column ["+colDesc.name+"] type [" + colDesc.kind + "] does not match the existing one type [" + cd.kind + "]");
                        // TODO: check for position
                    }

                    // check that the existing column matches the provided mapping
                    // TODO: do this check for collection
                    for(ColumnDescriptor colDesc : columnsList) {
                        if (colDesc.kind.isPrimaryKeyKind() || colDesc.type == null)
                            continue;

                        ColumnDefinition cdef = cfm.getColumnDefinition(new ColumnIdentifier(colDesc.name, true));
                        if (cdef != null && !cdef.type.isCollection()) {
                            String existingCqlType = cdef.type.asCQL3Type().toString();
                            if (colDesc.type.equals("frozen<geo_point>")) {
                                if (!(existingCqlType.equals("text") || existingCqlType.equals("frozen<geo_point>"))) {
                                    throw new ConfigurationException("geo_point cannot be mapped to column ["+colDesc.name+"] with CQL type ["+colDesc.type+"]. ");
                                }
                            } else {
                                // cdef.type.asCQL3Type() does not include frozen, nor quote, so can do this check for collection.
                                if (!existingCqlType.equals(colDesc.type) &&
                                    !colDesc.type.equals("frozen<"+existingCqlType+">") &&
                                    !(existingCqlType.endsWith("uuid") && colDesc.type.equals("text")) && // #74 uuid is mapped as keyword
                                    !(existingCqlType.equals("timeuuid") && (colDesc.type.equals("timestamp") || colDesc.type.equals("text"))) &&
                                    !(existingCqlType.equals("date") && colDesc.type.equals("timestamp")) &&
                                    !(existingCqlType.equals("time") && colDesc.type.equals("bigint"))
                                    ) // timeuuid can be mapped to date
                                throw new ConfigurationException("Existing column ["+colDesc.name+"] type ["+existingCqlType+"] mismatch with inferred type ["+colDesc.type+"]");
                            }
                        }
                    }
                }
                clusterService.updateMapping(mapperService.index().getName(), mappingMd);
            } else {
                // apply CQL schema update
                if (newTable) {
                    StringBuilder primaryKey = new StringBuilder();
                    primaryKey.append("(");
                    for(int i=0; i < primaryKeyLength; i++) {
                        primaryKey.append( ColumnIdentifier.maybeQuote(primaryKeyList[i].name) );
                        if ( i == partitionKeyLength -1)
                            primaryKey.append(")");
                        if (i+1 < primaryKeyLength )
                            primaryKey.append(",");
                    }
                    StringBuilder columnsDefinitions = new StringBuilder();
                    for(ColumnDescriptor colDesc : columnsList) {
                        columnsDefinitions.append( ColumnIdentifier.maybeQuote(colDesc.name) ).append(" ").append(colDesc.type);
                        if (colDesc.kind == Kind.STATIC)
                            columnsDefinitions.append(" static");
                        columnsDefinitions.append(",");
                    }
                    String query = String.format(Locale.ROOT, "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" ( %s PRIMARY KEY (%s) ) WITH COMMENT='Auto-created by Elassandra'",
                            ksName, cfName, columnsDefinitions.toString(), primaryKey.toString());
                    logger.debug(query);
                    QueryProcessor.executeInternal(query);
                    announce = true;
                } else {
                    // update an existing table
                    for(ColumnDescriptor colDesc : columnsList) {
                        if (colDesc.kind.isPrimaryKeyKind() || colDesc.type == null)
                            continue;

                        ColumnIdentifier cid = new ColumnIdentifier(colDesc.name, true);
                        ColumnDefinition cdef = cfm.getColumnDefinition(cid);
                        if (cdef == null) {
                            try {
                                String query = String.format(Locale.ROOT, "ALTER TABLE \"%s\".\"%s\" ADD %s %s", ksName, cfName, ColumnIdentifier.maybeQuote(colDesc.name), colDesc.type);
                                logger.debug(query);
                                QueryProcessor.executeInternal(query);
                                addedColumns.add(cid);
                            } catch (org.apache.cassandra.exceptions.InvalidRequestException e) {
                                logger.warn("Failed to ALTER TABLE {}.{} column {} {}", e, ksName, cfName, ColumnIdentifier.maybeQuote(colDesc.name), colDesc.type);
                            }
                        }
                    }
                }

                // announce CQL schema migration once for all modifications
                if (announce || addedColumns.size() > 0 || updatedUserTypes.size() > 0) {
                    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
                    KeyspaceMetadata keyspace = getKSMetaData(ksName);
                    CFMetaData newCfm = Schema.instance.getCFMetaData(ksName, cfName);
                    Mutation.SimpleBuilder builder = SchemaKeyspace.makeCreateKeyspaceMutation(ksName, keyspace.params, schemaUpdateTimestamp);

                    if (newTable)
                        SchemaKeyspace.addTableToSchemaMutation(newCfm, true, builder);
                    else
                        addedColumns.forEach(ci -> SchemaKeyspace.addColumnToSchemaMutation(newCfm, newCfm.getColumnDefinition(ci), builder));

                    updatedUserTypes.forEach(type -> SchemaKeyspace.addTypeToSchemaMutation(keyspace.types.getNullable(ByteBufferUtil.bytes(type)), builder));
                    MigrationManager.announce(builder, true);
                }
            }

        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    // see https://docs.datastax.com/en/cql/3.0/cql/cql_reference/keywords_r.html
    public static final Pattern keywordsPattern = Pattern.compile("(ADD|ALLOW|ALTER|AND|ANY|APPLY|ASC|AUTHORIZE|BATCH|BEGIN|BY|COLUMNFAMILY|CREATE|DELETE|DESC|DROP|EACH_QUORUM|GRANT|IN|INDEX|INET|INSERT|INTO|KEYSPACE|KEYSPACES|LIMIT|LOCAL_ONE|LOCAL_QUORUM|MODIFY|NOT|NORECURSIVE|OF|ON|ONE|ORDER|PASSWORD|PRIMARY|QUORUM|RENAME|REVOKE|SCHEMA|SELECT|SET|TABLE|TO|TOKEN|THREE|TRUNCATE|TWO|UNLOGGED|UPDATE|USE|USING|WHERE|WITH)");

    public static boolean isReservedKeyword(String identifier) {
        return keywordsPattern.matcher(identifier.toUpperCase(Locale.ROOT)).matches();
    }

    public static Pair<List<String>, List<String>> getUDTInfo(final String ksName, final String typeName) {
        try {
            UntypedResultSet result = QueryProcessor.executeOnceInternal("SELECT field_names, field_types FROM system_schema.types WHERE keyspace_name = ? AND type_name = ?",
                    new Object[] { ksName, typeName });
            Row row = result.one();
            if ((row != null) && row.has("field_names")) {
                List<String> field_names = row.getList("field_names", UTF8Type.instance);
                List<String> field_types = row.getList("field_types", UTF8Type.instance);
                return Pair.<List<String>, List<String>> create(field_names, field_types);
            }
        } catch (Exception e) {
        }
        return null;
    }

    public String buildUDT(final String ksName, final String cfName, final String name, final ObjectMapper objectMapper, Set<String> updatedUserTypes) throws RequestExecutionException {
        String typeName = (objectMapper.cqlUdtName() == null) ? cfName + "_" + objectMapper.fullPath().replace('.', '_') : objectMapper.cqlUdtName();

        if (!objectMapper.hasField()) {
            // delay UDT creation for object with no sub-field #146
            return null;
        }

        // create sub-type first
        for (Iterator<Mapper> it = objectMapper.iterator(); it.hasNext(); ) {
            Mapper mapper = it.next();
            if (mapper instanceof ObjectMapper) {
                buildCql(ksName, cfName, mapper.simpleName(), (ObjectMapper) mapper, updatedUserTypes);
            } else if (mapper instanceof GeoPointFieldMapper) {
                buildGeoPointType(ksName, updatedUserTypes);
            }
        }

        Pair<List<String>, List<String>> udt = getUDTInfo(ksName, typeName);
        if (udt == null) {
            // create new UDT.
            StringBuilder create = new StringBuilder(String.format(Locale.ROOT, "CREATE TYPE IF NOT EXISTS \"%s\".\"%s\" ( ", ksName, typeName));
            boolean first = true;
            for (Iterator<Mapper> it = objectMapper.iterator(); it.hasNext(); ) {
                Mapper mapper = it.next();
                if (mapper instanceof ObjectMapper && ((ObjectMapper) mapper).isEnabled() && !mapper.hasField()) {
                    continue;   // ignore object with no sub-field #146
                }
                if (first)
                    first = false;
                else
                    create.append(", ");

                // Use only the last part of the fullname to build UDT.
                int lastDotIndex = mapper.name().lastIndexOf('.');
                String shortName = (lastDotIndex > 0) ? mapper.name().substring(lastDotIndex+1) :  mapper.name();

                if (isReservedKeyword(shortName))
                    logger.warn("Allowing a CQL reserved keyword in ES: {}", shortName);

                create.append('\"').append(shortName).append("\" ");
                if (mapper instanceof ObjectMapper) {
                    if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                        create.append(mapper.cqlCollectionTag()).append("<");
                    create.append("frozen<")
                        .append(ColumnIdentifier.maybeQuote(cfName+'_'+((ObjectMapper) mapper).fullPath().replace('.', '_')))
                        .append(">");
                    if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                        create.append(">");
                } else if (mapper instanceof GeoPointFieldMapper) {
                    if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                        create.append(mapper.cqlCollectionTag()).append("<");
                        create.append("frozen<").append(GEO_POINT_TYPE).append(">");
                    if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                        create.append(">");
                } else if (mapper instanceof GeoShapeFieldMapper) {
                    if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                        create.append(mapper.cqlCollectionTag()).append("<");
                    create.append("text");
                    if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                        create.append(">");
                } else {
                    String cqlType = mapper.cqlType();
                    if (mapper.cqlCollection().equals(CqlCollection.SINGLETON)) {
                        create.append(cqlType);
                    } else {
                        create.append(mapper.cqlCollectionTag()).append("<");
                        if (!isNativeCql3Type(cqlType)) create.append("frozen<");
                        create.append(cqlType);
                        if (!isNativeCql3Type(cqlType)) create.append(">");
                        create.append(">");
                    }
                }
            }
            create.append(" )");
            if (!first) {
                logger.debug(create.toString());
                buildType(ksName, typeName, create.toString(), updatedUserTypes);
                return typeName;
            } else {
                // UDT not created because it has no sub-fields #146
                return null;
            }
        } else {
            // update existing UDT
            for (Iterator<Mapper> it = objectMapper.iterator(); it.hasNext(); ) {
                Mapper mapper = it.next();
                if (mapper instanceof ObjectMapper && ((ObjectMapper) mapper).isEnabled() && !((ObjectMapper) mapper).iterator().hasNext()) {
                    continue;   // ignore object with no sub-field #146
                }
                int lastDotIndex = mapper.name().lastIndexOf('.');
                String shortName = (lastDotIndex > 0) ? mapper.name().substring(lastDotIndex+1) :  mapper.name();
                if (isReservedKeyword(shortName))
                    logger.warn("Allowing a CQL reserved keyword in ES: {}", shortName);

                StringBuilder update = new StringBuilder(String.format(Locale.ROOT, "ALTER TYPE \"%s\".\"%s\" ADD \"%s\" ", ksName, typeName, shortName));
                if (!udt.left.contains(shortName)) {
                    if (mapper instanceof ObjectMapper) {
                        if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                            update.append(mapper.cqlCollectionTag()).append("<");
                        update.append("frozen<")
                            .append(cfName).append('_').append(((ObjectMapper) mapper).fullPath().replace('.', '_'))
                            .append(">");
                        if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                            update.append(">");
                    } else if (mapper instanceof GeoPointFieldMapper) {
                        if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                            update.append(mapper.cqlCollectionTag()).append("<");
                        update.append("frozen<")
                            .append(GEO_POINT_TYPE)
                            .append(">");
                        if (!mapper.cqlCollection().equals(CqlCollection.SINGLETON))
                            update.append(">");
                    } else {
                        String cqlType = mapper.cqlType();
                        if (mapper.cqlCollection().equals(CqlCollection.SINGLETON)) {
                            update.append(cqlType);
                        } else {
                            update.append(mapper.cqlCollectionTag()).append("<");
                            if (!isNativeCql3Type(cqlType)) update.append("frozen<");
                            update.append(cqlType);
                            if (!isNativeCql3Type(cqlType)) update.append(">");
                            update.append(">");
                        }
                    }
                    logger.debug(update.toString());
                    QueryProcessor.executeInternal(update.toString());
                    updatedUserTypes.add(typeName);
                }
            }
        }
        return typeName;
    }

    public String buildCql(final String ksName, final String cfName, final String name, final ObjectMapper objectMapper, Set<String> updatedUserTypes) throws RequestExecutionException {
        if (objectMapper.cqlStruct().equals(CqlStruct.UDT) && objectMapper.iterator().hasNext()) {
            return buildUDT(ksName, cfName, name, objectMapper, updatedUserTypes);
        } else if (objectMapper.cqlStruct().equals(CqlStruct.MAP) && objectMapper.iterator().hasNext()) {
            if (objectMapper.iterator().hasNext()) {
                Mapper childMapper = objectMapper.iterator().next();
                if (childMapper instanceof FieldMapper) {
                    return "map<text,"+childMapper.cqlType()+">";
                } else if (childMapper instanceof ObjectMapper) {
                    String subType = buildCql(ksName,cfName,childMapper.simpleName(),(ObjectMapper)childMapper, updatedUserTypes);
                    return (subType==null) ? null : "map<text,frozen<"+subType+">>";
                }
            } else {
                // default map prototype, no mapper to determine the value type.
                return "map<text,text>";
            }
        }
        return null;
    }

    public void createSecondaryIndices(final IndexMetaData indexMetaData) throws IOException {
        String ksName = indexMetaData.keyspace();
        String className = indexMetaData.getSettings().get(IndexMetaData.SETTING_SECONDARY_INDEX_CLASS, this.settings.get(ClusterService.SETTING_CLUSTER_SECONDARY_INDEX_CLASS, ClusterService.defaultSecondaryIndexClass.getName()));
        for(ObjectCursor<MappingMetaData> cursor: indexMetaData.getMappings().values()) {
            createSecondaryIndex(ksName, cursor.value, className);
        }
    }

    // build secondary indices when shard is started and mapping applied
    public void createSecondaryIndex(String ksName, MappingMetaData mapping, String className) throws IOException {
        final String cfName = SchemaManager.typeToCfName(ksName, mapping.type());
        final CFMetaData cfm = Schema.instance.getCFMetaData(ksName, cfName);
        boolean found = false;
        if (cfm != null && cfm.getIndexes() != null) {
            for(IndexMetadata indexMetadata : cfm.getIndexes()) {
                if (indexMetadata.isCustom() && indexMetadata.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME).equals(className)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                String indexName = buildIndexName(cfName);
                String query = String.format(Locale.ROOT, "CREATE CUSTOM INDEX IF NOT EXISTS \"%s\" ON \"%s\".\"%s\" () USING '%s'",
                        indexName, ksName, cfName, className);
                logger.debug(query);
                try {
                    QueryProcessor.process(query, ConsistencyLevel.LOCAL_ONE);
                } catch (Throwable e) {
                    throw new IOException("Failed to process query=["+query+"]:"+e.getMessage(), e);
                }
            }
        } else {
            logger.warn("Cannot create SECONDARY INDEX, [{}.{}] does not exist",ksName, cfName);
        }
    }

    public void dropSecondaryIndices(final IndexMetaData indexMetaData) throws RequestExecutionException {
        String ksName = indexMetaData.keyspace();
        for(CFMetaData cfMetaData : Schema.instance.getKSMetaData(ksName).tablesAndViews()) {
            if (cfMetaData.isCQLTable())
                dropSecondaryIndex(cfMetaData);
        }
    }

    public void dropSecondaryIndex(String ksName, String cfName) throws RequestExecutionException  {
        CFMetaData cfMetaData = Schema.instance.getCFMetaData(ksName, cfName);
        if (cfMetaData != null)
            dropSecondaryIndex(cfMetaData);
    }

    public void dropSecondaryIndex(CFMetaData cfMetaData) throws RequestExecutionException  {
        for(IndexMetadata idx : cfMetaData.getIndexes()) {
            if (idx.isCustom()) {
                String className = idx.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
                if (className != null && className.endsWith("ElasticSecondaryIndex")) {
                    logger.debug("DROP INDEX IF EXISTS {}.{}", cfMetaData.ksName, idx.name);
                    QueryProcessor.process(String.format(Locale.ROOT, "DROP INDEX IF EXISTS \"%s\".\"%s\"",
                            cfMetaData.ksName, idx.name),
                            ConsistencyLevel.LOCAL_ONE);
                }
            }
        }
    }

    public void dropTables(final IndexMetaData indexMetaData) throws RequestExecutionException {
        String ksName = indexMetaData.keyspace();
        for(ObjectCursor<String> cfName : indexMetaData.getMappings().keys()) {
            dropTable(ksName, cfName.value);
        }
    }

    public void dropTable(String ksName, String cfName) throws RequestExecutionException  {
        CFMetaData cfm = Schema.instance.getCFMetaData(ksName, cfName);
        if (cfm != null) {
            logger.warn("DROP TABLE IF EXISTS {}.{}", ksName, cfName);
            QueryProcessor.process(String.format(Locale.ROOT, "DROP TABLE IF EXISTS \"%s\".\"%s\"", ksName, cfName), ConsistencyLevel.LOCAL_ONE);
        }
    }

    public static String buildIndexName(final String cfName) {
        return new StringBuilder("elastic_")
            .append(cfName)
            .append("_idx").toString();
    }

    public static String buildIndexName(final String cfName, final String colName) {
        return new StringBuilder("elastic_")
            .append(cfName).append('_')
            .append(colName.replaceAll("\\W+", "_"))
            .append("_idx").toString();
    }
}
