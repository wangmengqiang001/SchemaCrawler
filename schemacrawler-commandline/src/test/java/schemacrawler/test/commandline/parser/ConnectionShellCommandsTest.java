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
package schemacrawler.test.commandline.parser;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static schemacrawler.test.utility.FileHasContent.hasNoContent;
import static schemacrawler.test.utility.FileHasContent.outputOf;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;
import schemacrawler.schemacrawler.Config;
import schemacrawler.test.utility.DatabaseConnectionInfo;
import schemacrawler.test.utility.TestDatabaseConnectionParameterResolver;
import schemacrawler.test.utility.TestOutputStream;
import schemacrawler.tools.commandline.shell.DisconnectCommand;
import schemacrawler.tools.commandline.shell.IsConnectedCommand;
import schemacrawler.tools.commandline.shell.SweepCommand;
import schemacrawler.tools.commandline.state.SchemaCrawlerShellState;

@ExtendWith(TestDatabaseConnectionParameterResolver.class)
public class ConnectionShellCommandsTest
{

  private static BasicDataSource createDataSource(final DatabaseConnectionInfo connectionInfo)
  {
    final BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername("sa");
    dataSource.setPassword("");
    dataSource.setUrl(connectionInfo.getConnectionUrl());
    dataSource.setDefaultAutoCommit(false);
    dataSource.setInitialSize(1);
    dataSource.setMaxTotal(1);
    return dataSource;
  }

  private TestOutputStream err;
  private TestOutputStream out;

  @AfterEach
  public void cleanUpStreams()
  {
    System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
    System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
  }

  @BeforeEach
  public void setUpStreams()
    throws Exception
  {
    out = new TestOutputStream();
    System.setOut(new PrintStream(out));

    err = new TestOutputStream();
    System.setErr(new PrintStream(err));
  }

  @Test
  public void isConnected(final DatabaseConnectionInfo connectionInfo)
  {
    final BasicDataSource dataSource = createDataSource(connectionInfo);

    final SchemaCrawlerShellState state = new SchemaCrawlerShellState();
    state.setDataSource(dataSource);

    final String[] args = new String[0];

    final IsConnectedCommand optionsParser = new IsConnectedCommand(state);
    CommandLine.run(optionsParser, args);

    assertThat(outputOf(err), hasNoContent());
    assertThat(out.getFileContents(), startsWith("connected"));
  }

  @Test
  public void isNotConnected(final DatabaseConnectionInfo connectionInfo)
  {
    final SchemaCrawlerShellState state = new SchemaCrawlerShellState();

    final String[] args = new String[0];

    final IsConnectedCommand optionsParser = new IsConnectedCommand(state);
    CommandLine.run(optionsParser, args);

    assertThat(outputOf(err), hasNoContent());
    assertThat(out.getFileContents(), startsWith("not connected"));
  }

  @Test
  public void disconnect(final DatabaseConnectionInfo connectionInfo)
  {
    final BasicDataSource dataSource = createDataSource(connectionInfo);

    final SchemaCrawlerShellState state = new SchemaCrawlerShellState();
    state.setDataSource(dataSource);

    final String[] args = new String[0];

    assertThat(state.getDataSource(), is(not(nullValue())));

    final DisconnectCommand disconnectCommand = new DisconnectCommand(state);
    CommandLine.run(disconnectCommand, args);

    assertThat(state.getDataSource(), is(nullValue()));
  }

  @Test
  public void disconnectWhenNotConnected(final DatabaseConnectionInfo connectionInfo)
    throws Exception
  {

    final SchemaCrawlerShellState state = new SchemaCrawlerShellState();

    final String[] args = new String[0];

    assertThat(state.getDataSource(), is(nullValue()));

    final DisconnectCommand disconnectCommand = new DisconnectCommand(state);
    CommandLine.run(disconnectCommand, args);

    assertThat(state.getDataSource(), is(nullValue()));
  }

  @Test
  public void sweep()
  {

    final SchemaCrawlerShellState state = new SchemaCrawlerShellState();
    state.setAdditionalConfiguration(new Config());

    final String[] args = new String[0];

    assertThat(state.getAdditionalConfiguration(), is(not(nullValue())));

    final SweepCommand sweepCommand = new SweepCommand(state);
    CommandLine.run(sweepCommand, args);

    assertThat(state.getAdditionalConfiguration(), is(nullValue()));
  }

  @Test
  public void sweepWithNoState()
  {

    final SchemaCrawlerShellState state = new SchemaCrawlerShellState();

    final String[] args = new String[0];

    assertThat(state.getAdditionalConfiguration(), is(nullValue()));

    final SweepCommand sweepCommand = new SweepCommand(state);
    CommandLine.run(sweepCommand, args);

    assertThat(state.getAdditionalConfiguration(), is(nullValue()));
  }

}
