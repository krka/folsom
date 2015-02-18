/*
 * Copyright (c) 2014-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.folsom.ketama;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.spotify.folsom.RawMemcacheClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static com.spotify.folsom.ByteEncoders.utf8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ContinuumTest {

  private static final HostAndPort ADDRESS1 = HostAndPort.fromParts("127.0.0.1", 11211);
  private static final HostAndPort ADDRESS2 = HostAndPort.fromParts("127.0.0.1", 11212);
  private static final HostAndPort ADDRESS3 = HostAndPort.fromParts("127.0.0.1", 11213);
  private static final HostAndPort ADDRESS4 = HostAndPort.fromParts("127.0.0.1", 11214);
  private static final HostAndPort ADDRESS5 = HostAndPort.fromParts("127.0.0.1", 11215);

  private static final RawMemcacheClient CLIENT1 = mock(RawMemcacheClient.class);
  private static final RawMemcacheClient CLIENT2 = mock(RawMemcacheClient.class);
  private static final RawMemcacheClient CLIENT3 = mock(RawMemcacheClient.class);
  private static final RawMemcacheClient CLIENT4 = mock(RawMemcacheClient.class);
  private static final RawMemcacheClient CLIENT5 = mock(RawMemcacheClient.class);

  private static final AddressAndClient AAC1 = new AddressAndClient(ADDRESS1, CLIENT1);
  private static final AddressAndClient AAC2 = new AddressAndClient(ADDRESS2, CLIENT2);
  private static final AddressAndClient AAC3 = new AddressAndClient(ADDRESS3, CLIENT3);
  private static final AddressAndClient AAC4 = new AddressAndClient(ADDRESS4, CLIENT4);
  private static final AddressAndClient AAC5 = new AddressAndClient(ADDRESS5, CLIENT5);

  private static final byte[] KEY4 = utf8("key4");
  private static final byte[] KEY1 = utf8("key1");
  private static final byte[] KEY2 = utf8("key2");
  private static final byte[] KEY3 = utf8("key3");
  private static final byte[] KEY5 = utf8("key5");
  private static final byte[] KEY6 = utf8("key6");
  private static final byte[] KEY7 = utf8("key7");
  private static final byte[] KEY8 = utf8("key8");
  private static final byte[] KEY9 = utf8("key9");
  private static final byte[] KEY10 = utf8("key10");

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(CLIENT1.isConnected()).thenReturn(true);
    when(CLIENT2.isConnected()).thenReturn(true);
    when(CLIENT3.isConnected()).thenReturn(true);
    when(CLIENT4.isConnected()).thenReturn(true);
    when(CLIENT5.isConnected()).thenReturn(true);
  }

  @Test
  public void testSingleClient() {
    List<AddressAndClient> clients = ImmutableList.of(AAC1);
    Continuum c = new Continuum(clients);

    List<RawMemcacheClient> actual = Arrays.asList(
            c.findClient(KEY1),
            c.findClient(KEY2),
            c.findClient(KEY3));

    // all keys to the same client
    List<RawMemcacheClient> expected = Arrays.asList(CLIENT1, CLIENT1, CLIENT1);
    assertEquals(expected, actual);

  }

  @Test
  public void testMultipleClients() {
    List<AddressAndClient> clients = ImmutableList.of(AAC1, AAC2, AAC3);
    Continuum c = new Continuum(clients);

    List<RawMemcacheClient> actual = Arrays.asList(
            c.findClient(KEY1),
            c.findClient(KEY2),
            c.findClient(KEY3),
            c.findClient(KEY4),
            c.findClient(KEY5),
            c.findClient(KEY6));

    List<RawMemcacheClient> expected = Arrays.asList(
            CLIENT1,
            CLIENT1,
            CLIENT2,
            CLIENT3,
            CLIENT2,
            CLIENT1);
    assertEquals(expected, actual);
  }

  @Test
  public void testMultipleClientsOneDisconnected() {
    List<AddressAndClient> clients = ImmutableList.of(AAC1, AAC2, AAC3, AAC4, AAC5);
    Continuum c = new Continuum(clients);

    List<RawMemcacheClient> actual = Arrays.asList(
            c.findClient(KEY1),
            c.findClient(KEY2),
            c.findClient(KEY3),
            c.findClient(KEY4),
            c.findClient(KEY5),
            c.findClient(KEY6),
            c.findClient(KEY7),
            c.findClient(KEY8),
            c.findClient(KEY9),
            c.findClient(KEY10)
    );


    List<RawMemcacheClient> expected = Arrays.asList(
            CLIENT1,
            CLIENT5,
            CLIENT2,
            CLIENT4,
            CLIENT2,
            CLIENT1,
            CLIENT5,
            CLIENT1,
            CLIENT1,
            CLIENT4);
    assertEquals(expected, actual);

    when(CLIENT1.isConnected()).thenReturn(false);

    actual = Arrays.asList(
            c.findClient(KEY1),
            c.findClient(KEY2),
            c.findClient(KEY3),
            c.findClient(KEY4),
            c.findClient(KEY5),
            c.findClient(KEY6),
            c.findClient(KEY7),
            c.findClient(KEY8),
            c.findClient(KEY9),
            c.findClient(KEY10)
    );


    expected = Arrays.asList(
            CLIENT4, // 1 -> 4
            CLIENT5,
            CLIENT2,
            CLIENT4,
            CLIENT2,
            CLIENT5, // 1 -> 5
            CLIENT5,
            CLIENT2, // 1 -> 2
            CLIENT5, // 1 -> 5
            CLIENT4);
    assertEquals(expected, actual);
  }

  @Test
  public void testWrap() {
    // key321 hashes to a value bigger than any node in the ring, thus we must wrap around the ring
    // key477 is smaller than any node in the ring, and thus also should get the same client

    final List<AddressAndClient> clients = ImmutableList.of(AAC1, AAC2, AAC3);
    final Continuum c = new Continuum(clients);

    List<RawMemcacheClient> actual = Arrays.asList(c.findClient(utf8("key321")),
                                                   c.findClient(utf8("key477")));
    List<RawMemcacheClient> expected = Arrays.asList(CLIENT2, CLIENT3);
    assertEquals(expected, actual);
  }

  @Test
  public void testWrapDisconnected() {
    // key1561 hashes to CLIENT3, which is the last node in the rung, but is disconnected.
    // thus we must wrap around correctly in the disconnected case

    final List<AddressAndClient> clients = ImmutableList.of(AAC1, AAC2, AAC3);
    final Continuum c = new Continuum(clients);

    assertSame(CLIENT1, c.findClient(utf8("key1561")));

    when(CLIENT1.isConnected()).thenReturn(false);
    assertSame(CLIENT2, c.findClient(utf8("key1561")));
  }
}
