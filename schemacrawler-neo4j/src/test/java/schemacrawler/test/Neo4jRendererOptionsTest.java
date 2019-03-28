package schemacrawler.test;/*
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


import static schemacrawler.test.utility.ExecutableTestUtility.executableExecution;
import static schemacrawler.test.utility.TestUtility.clean;

import java.nio.file.Path;
import java.sql.Connection;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import schemacrawler.schemacrawler.IncludeAll;
import schemacrawler.schemacrawler.RegularExpressionExclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SchemaCrawlerOptionsBuilder;
import schemacrawler.test.utility.*;
import schemacrawler.tools.executable.SchemaCrawlerExecutable;
import schemacrawler.tools.options.TextOutputFormat;

@ExtendWith(TestLoggingExtension.class)
@ExtendWith(TestDatabaseConnectionParameterResolver.class)
@ExtendWith(TestContextParameterResolver.class)
public class Neo4jRendererOptionsTest
{

  private static final String NEO4J_OUTPUT = "neo4j_output/";

  @BeforeAll
  public static void removeOutputDir()
    throws Exception
  {
    clean(NEO4J_OUTPUT);
  }

  @Test
  public void executableForNeo4j_00(final TestContext testContext,
                                    final Connection connection)
    throws Exception
  {
    final SchemaCrawlerOptions schemaCrawlerOptions = DatabaseTestUtility.schemaCrawlerOptionsWithMaximumSchemaInfoLevel;

    executableNeo4j(connection,
                    schemaCrawlerOptions,
                    testContext.testMethodName());
  }

  private void executableNeo4j(final Connection connection,
                               final SchemaCrawlerOptions options,
                               final String testMethodName)
    throws Exception
  {
    SchemaCrawlerOptions schemaCrawlerOptions = options;
    if (options.getSchemaInclusionRule().equals(new IncludeAll()))
    {
      schemaCrawlerOptions = SchemaCrawlerOptionsBuilder.builder()
        .fromOptions(options).includeSchemas(new RegularExpressionExclusionRule(
          ".*\\.SYSTEM_LOBS|.*\\.FOR_LINT")).toOptions();
    }

    final SchemaCrawlerExecutable executable = new SchemaCrawlerExecutable(
      "neo4j");
    executable.setSchemaCrawlerOptions(schemaCrawlerOptions);

    final Path testDiagramFile = executableExecution(connection,
                                                     executable,
                                                     TextOutputFormat.text);
  }

}
