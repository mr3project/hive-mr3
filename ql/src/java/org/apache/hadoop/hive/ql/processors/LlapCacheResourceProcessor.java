/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.processors;

import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;
import static org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.defaultNullString;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.llap.daemon.rpc.MR3LlapDaemonProtocolProtos.MR3LlapDaemonProcessorEventProto;
import org.apache.hadoop.hive.llap.daemon.rpc.MR3LlapDaemonProtocolProtos.MR3LlapDaemonProcessorEventType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.mr3.llap.LLAPDaemonProcessor;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3Session;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class LlapCacheResourceProcessor implements CommandProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(LlapCacheResourceProcessor.class);
  private Options CACHE_OPTIONS = new Options();
  private HelpFormatter helpFormatter = new HelpFormatter();

  LlapCacheResourceProcessor() {
    CACHE_OPTIONS.addOption("purge", "purge", false, "Purge LLAP IO cache");
  }

  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    SessionState ss = SessionState.get();
    command = new VariableSubstitution(() -> SessionState.get().getHiveVariables()).substitute(ss.getConf(), command);
    String[] tokens = command.split("\\s+");
    if (tokens.length < 1) {
      throw new CommandProcessorException("LLAP Cache Processor Helper Failed: Command arguments are empty.");
    }
    String params[] = Arrays.copyOfRange(tokens, 1, tokens.length);
    try {
      return llapCacheCommandHandler(ss, params);
    } catch (CommandProcessorException e) {
      throw e;
    } catch (Exception e) {
      throw new CommandProcessorException("LLAP Cache Processor Helper Failed: " + e.getMessage());
    }
  }

  private CommandProcessorResponse llapCacheCommandHandler(SessionState ss, String[] params)
      throws ParseException, CommandProcessorException {
    CommandLine args = parseCommandArgs(CACHE_OPTIONS, params);
    boolean purge = args.hasOption("purge");
    String hs2Host = null;
    if (ss.isHiveServerQuery()) {
      hs2Host = ss.getHiveServer2Host();
    }
    if (purge) {
      List<String> fullCommand = Lists.newArrayList("llap", "cache");
      fullCommand.addAll(Arrays.asList(params));
      CommandProcessorResponse authErrResp =
        CommandUtil.authorizeCommandAndServiceObject(ss, HiveOperationType.LLAP_CACHE_PURGE, fullCommand, hs2Host);
      if (authErrResp != null) {
        // there was an authorization issue
        return authErrResp;
      }
      try {
        llapCachePurge(ss);
        return new CommandProcessorResponse(getSchema(), null);
      } catch (Exception e) {
        LOG.error("Error while purging LLAP IO Cache. err: ", e);
        throw new CommandProcessorException(
            "LLAP Cache Processor Helper Failed: Error while purging LLAP IO Cache. err: " + e.getMessage());
      }
    } else {
      String usage = getUsageAsString();
      throw new CommandProcessorException(
          "LLAP Cache Processor Helper Failed: Unsupported sub-command option. " + usage);
    }
  }

  private Schema getSchema() {
    Schema sch = new Schema();
    sch.addToFieldSchemas(new FieldSchema("hostName", STRING_TYPE_NAME, ""));
    sch.addToFieldSchemas(new FieldSchema("purgedMemoryBytes", STRING_TYPE_NAME, ""));
    sch.putToProperties(SERIALIZATION_NULL_FORMAT, defaultNullString);
    return sch;
  }

  private void llapCachePurge(final SessionState ss) throws Exception {
    MR3Session mr3Session = ss.getMr3Session();
    if (mr3Session == null) {
      LOG.warn("Skip LLAP cache purge as MR3Session is not ready.");
      return;
    }

    MR3LlapDaemonProcessorEventProto eventProto = MR3LlapDaemonProcessorEventProto.newBuilder()
        .setType(MR3LlapDaemonProcessorEventType.PURGE)
        .build();
    ByteString payload = eventProto.toByteString();

    mr3Session.sendDaemonMessage(LLAPDaemonProcessor.daemonVertexName, payload);
  }

  private String getUsageAsString() {
    StringWriter out = new StringWriter();
    PrintWriter pw = new PrintWriter(out);
    helpFormatter.printUsage(pw, helpFormatter.getWidth(), "llap cache", CACHE_OPTIONS);
    pw.flush();
    return out.toString();
  }

  private CommandLine parseCommandArgs(final Options opts, String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();
    return parser.parse(opts, args);
  }

  @Override
  public void close() {
  }
}
