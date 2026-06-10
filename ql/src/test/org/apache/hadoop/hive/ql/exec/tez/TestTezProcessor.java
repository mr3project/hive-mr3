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
package org.apache.hadoop.hive.ql.exec.tez;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValuesWriterEdge;
import org.apache.tez.runtime.library.api.KeyValuesWriterEdgeVector;
import org.apache.tez.runtime.library.api.LogicalOutputEdge;
import org.junit.Assert;
import org.junit.Test;

public class TestTezProcessor {

  @Test
  public void testCurrentUserCredentialsAreMergedOnInitialize() throws IOException {
    ProcessorContext processorContext = mockProcessorContext();
    TezProcessor tezProcessor = new TezProcessor(processorContext);
    Text tokenId = new Text("TOKEN_KIND");

    Token<TokenIdentifier> token =
        new Token<>("hello".getBytes(), null, new Text("TEST_TOKEN_KIND"), new Text("TEST_TOKEN_SERVICE"));
    UserGroupInformation.getCurrentUser().addToken(tokenId, token);

    tezProcessor.initialize();
    JobConf conf = tezProcessor.getConf();
    Assert.assertEquals(token, conf.getCredentials().getToken(tokenId));
  }

  @Test
  public void testVectorBatchOutputCollectorDelegatesToEdgeWriter() throws Exception {
    LogicalOutputEdge output = mock(LogicalOutputEdge.class);
    KeyValuesWriterEdge writer = mock(KeyValuesWriterEdge.class,
        withSettings().extraInterfaces(KeyValuesWriterEdgeVector.class));
    KeyValuesWriterEdgeVector vectorWriter = (KeyValuesWriterEdgeVector) writer;
    BytesWritable serializedBatch = new BytesWritable(new byte[] {1, 2, 3});
    when(output.getWriter()).thenReturn(writer);
    when(vectorWriter.supportsVectorBatch()).thenReturn(true);

    TezProcessor.TezKVOutputCollector collector = new TezProcessor.TezKVOutputCollector(output);
    collector.initialize();

    Assert.assertTrue(collector.supportsVectorBatch());
    collector.writeVectorBatch(serializedBatch);
    verify(vectorWriter).writeVectorBatch(serializedBatch);
  }

  @Test(expected = IllegalStateException.class)
  public void testVectorBatchOutputCollectorRejectsOrdinaryWriteAfterVectorWrite() throws Exception {
    LogicalOutputEdge output = mock(LogicalOutputEdge.class);
    KeyValuesWriterEdge writer = mock(KeyValuesWriterEdge.class,
        withSettings().extraInterfaces(KeyValuesWriterEdgeVector.class));
    KeyValuesWriterEdgeVector vectorWriter = (KeyValuesWriterEdgeVector) writer;
    when(output.getWriter()).thenReturn(writer);
    when(vectorWriter.supportsVectorBatch()).thenReturn(true);

    TezProcessor.TezKVOutputCollector collector = new TezProcessor.TezKVOutputCollector(output);
    collector.initialize();
    collector.writeVectorBatch(new BytesWritable());
    collector.collect(new BytesWritable(), new BytesWritable());
  }

  @Test(expected = IllegalStateException.class)
  public void testVectorBatchOutputCollectorRejectsVectorWriteAfterOrdinaryWrite() throws Exception {
    LogicalOutputEdge output = mock(LogicalOutputEdge.class);
    KeyValuesWriterEdge writer = mock(KeyValuesWriterEdge.class,
        withSettings().extraInterfaces(KeyValuesWriterEdgeVector.class));
    KeyValuesWriterEdgeVector vectorWriter = (KeyValuesWriterEdgeVector) writer;
    when(output.getWriter()).thenReturn(writer);
    when(vectorWriter.supportsVectorBatch()).thenReturn(true);

    TezProcessor.TezKVOutputCollector collector = new TezProcessor.TezKVOutputCollector(output);
    collector.initialize();
    collector.collect(new BytesWritable(), new BytesWritable());
    collector.writeVectorBatch(new BytesWritable());
  }

  @Test
  public void testNonVectorWriterDoesNotAdvertiseVectorBatches() throws Exception {
    LogicalOutputEdge output = mock(LogicalOutputEdge.class);
    KeyValuesWriterEdge writer = mock(KeyValuesWriterEdge.class);
    when(output.getWriter()).thenReturn(writer);

    TezProcessor.TezKVOutputCollector collector = new TezProcessor.TezKVOutputCollector(output);
    collector.initialize();

    Assert.assertFalse(collector.supportsVectorBatch());
  }

  private ProcessorContext mockProcessorContext() throws IOException {
    JobConf conf = new JobConf();
    ProcessorContext processorContext = mock(ProcessorContext.class);
    when(processorContext.getUserPayload()).thenReturn(TezUtils.createUserPayloadFromConf(conf));

    when(processorContext.getTaskVertexName()).thenReturn("Map 1");
    when(processorContext.getTaskVertexIndex()).thenReturn(0);
    when(processorContext.getTaskIndex()).thenReturn(0);
    when(processorContext.getTaskAttemptNumber()).thenReturn(0);
    when(processorContext.getApplicationId()).thenReturn(ApplicationId.fromString("application_123456_0"));
    when(processorContext.getUniqueIdentifier()).thenReturn("attempt_97501718590653602_0001_1_01_000000_0");

    return processorContext;
  }
}
