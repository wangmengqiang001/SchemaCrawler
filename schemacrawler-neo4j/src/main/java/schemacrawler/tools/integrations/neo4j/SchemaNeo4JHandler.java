/*
========================================================================
SchemaCrawler
http://www.schemacrawler.com
Copyright (c) 2000-2019, Sualeh Fatehi <sualeh@hotmail.com>.
All rights reserved.
------------------------------------------------------------------------

SchemaCrawler is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

SchemaCrawler and the accompanying materials are made available under
the terms of the Eclipse Public License v1.0, GNU General Public License
v3 or GNU Lesser General Public License v3.

You may elect to redistribute this code under any of these licenses.

The Eclipse Public License is available at:
http://www.eclipse.org/legal/epl-v10.html

The GNU General Public License v3 and the GNU Lesser General Public
License v3 are available at:
http://www.gnu.org/licenses/

========================================================================
*/
package schemacrawler.tools.integrations.neo4j;


import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import apoc.ApocConfiguration;
import apoc.export.cypher.ExportCypher;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import schemacrawler.SchemaCrawlerInfo;
import schemacrawler.schema.*;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.tools.traversal.SchemaTraversalHandler;

public class SchemaNeo4JHandler
  implements SchemaTraversalHandler
{

  private final GraphDatabaseService dbService;

  public SchemaNeo4JHandler(final Path outputDirectory)
  {
    requireNonNull(outputDirectory, "No output directory provided");
    System.out.println(outputDirectory.toAbsolutePath());

    final GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
    dbService = dbFactory.newEmbeddedDatabase(outputDirectory.toFile());
  }

  @Override
  public void handle(final ColumnDataType columnDataType)
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handle(final Routine routine)
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handle(final Sequence sequence)
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handle(final Synonym synonym)
    throws SchemaCrawlerException
  {

  }

  private void handleRemarks(final DescribedObject object, final Node node)
  {
    if (object == null)
    {
      return;
    }
    // Print empty string, if value is not available
    node.setProperty("remarks", object.getRemarks());
  }

  @Override
  public void handle(final Table table)
    throws SchemaCrawlerException
  {
    try (final Transaction tx = dbService.beginTx())
    {
      // Create node for schema
      final Schema schema = table.getSchema();
      Node schemaNode = dbService
        .findNode(DatabaseObjectLabel.Schema, "fullName", schema.getFullName());
      if (schemaNode == null)
      {
        schemaNode = dbService.createNode(DatabaseObjectLabel.Schema);
      }
      schemaNode.setProperty("name", schema.getName());
      schemaNode.setProperty("fullName", schema.getFullName());
      handleRemarks(schema, schemaNode);

      final Node tableNode = dbService.createNode(DatabaseObjectLabel.Table);
      tableNode.setProperty("name", table.getName());
      tableNode.setProperty("fullName", table.getFullName());
      tableNode.setProperty("type", table.getTableType().getTableType());
      handleRemarks(table, tableNode);

      schemaNode
        .createRelationshipTo(tableNode, SchemaRelationshipType.CONTAINS);
      tableNode
        .createRelationshipTo(schemaNode, SchemaRelationshipType.BELONGS_TO);

      for (final Column column : table.getColumns())
      {
        handleTableColumn(tableNode, column);
      }
      // TODO: Handle hidden columns

      handleDefinition(table, tableNode);

      tx.success();
    }
  }

  private void handleTableColumn(final Node tableNode, final Column column)
  {
    final Node columnNode = dbService
      .createNode(DatabaseObjectLabel.TableColumn);

    columnNode.setProperty("name", column.getName());
    columnNode.setProperty("fullName", column.getFullName());
    columnNode.setProperty("shortName", column.getShortName());
    handleRemarks(column, columnNode);

    if (column instanceof IndexColumn)
    {
      columnNode.setProperty("sortSequence",
                             ((IndexColumn) column).getSortSequence().name());
    }
    else
    {
      columnNode.setProperty("dataType",
                             column.getColumnDataType().getJavaSqlType()
                               .getName());
      columnNode.setProperty("databaseSpecificType",
                             column.getColumnDataType()
                               .getDatabaseSpecificTypeName());
      columnNode.setProperty("width", column.getWidth());
      columnNode.setProperty("size", column.getSize());
      columnNode.setProperty("decimalDigits", column.getDecimalDigits());
      columnNode.setProperty("nullable", column.isNullable());
      columnNode.setProperty("autoIncremented", column.isAutoIncremented());
      columnNode.setProperty("generated", column.isGenerated());
    }

    columnNode.setProperty("ordinal", column.getOrdinalPosition());

    if (column instanceof DefinedObject)
    {
      handleDefinition((DefinedObject) column, columnNode);
    }

    tableNode.createRelationshipTo(columnNode, SchemaRelationshipType.CONTAINS);
    columnNode
      .createRelationshipTo(tableNode, SchemaRelationshipType.BELONGS_TO);
  }

  private void handleDefinition(final DefinedObject definedObject,
                                final Node node)
  {
    // Print empty string, if value is not available
    node.setProperty("definition", definedObject.getDefinition());
  }

  @Override
  public void handleColumnDataTypesEnd()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleColumnDataTypesStart()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleRoutinesEnd()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleRoutinesStart()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleSequencesEnd()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleSequencesStart()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleSynonymsEnd()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleSynonymsStart()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleTablesEnd()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleTablesStart()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void begin()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void end()
    throws SchemaCrawlerException
  {
    try (final Transaction tx = dbService.beginTx())
    {
      final Map<String, Object> config = new HashMap<>();
      config.put("export.file.enabled", "true");
      ApocConfiguration.addToConfig(config);

      final ExportCypher exportCypher = new ExportCypher(dbService);
      exportCypher.all("./sc.cypher", config);

      tx.success();
    }
    catch (final IOException e)
    {
      throw new SchemaCrawlerException("Could not export Cypher file", e);
    }

    if (dbService != null)
    {
      dbService.shutdown();
    }
  }

  @Override
  public void handle(final CrawlInfo crawlInfo)
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handle(final DatabaseInfo databaseInfo)
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handle(final JdbcDriverInfo jdbcDriverInfo)
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handle(final SchemaCrawlerInfo schemaCrawlerInfo)
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleHeaderEnd()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleHeaderStart()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleInfoEnd()
    throws SchemaCrawlerException
  {

  }

  @Override
  public void handleInfoStart()
    throws SchemaCrawlerException
  {

  }
}
