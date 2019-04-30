package com.spotify.folsom.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.testcontainers.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;

public class UtilsTest {
  @Test
  public void testTtlToExpiration() {
    assertEquals(0, Utils.ttlToExpiration(-123123));
    assertEquals(0, Utils.ttlToExpiration(0));
    assertEquals(123, Utils.ttlToExpiration(123));

    assertEquals(2591999, Utils.ttlToExpiration(2591999));

    // Converted to timestamp
    assertTimestamp(2592000);
    assertTimestamp(3000000);

    // Overflows the timestamp
    assertEquals(Integer.MAX_VALUE - 1, Utils.ttlToExpiration(Integer.MAX_VALUE - 1234));
  }

  private void assertTimestamp(int ttl) {
    double expectedTimestamp = (System.currentTimeMillis() / 1000.0) + ttl;
    assertEquals(expectedTimestamp, Utils.ttlToExpiration(ttl), 2.0);
  }

  @Test
  public void testOnExecutorNotCompletedYetAsync() throws ExecutionException, InterruptedException {
    ExecutorService executorA =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("thread-A-%d").setDaemon(true).build());
    ExecutorService executorB =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("thread-B-%d").setDaemon(true).build());
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<String> future2 =
        Utils.onExecutor(future, executorB)
            .toCompletableFuture()
            .thenApply(s -> Thread.currentThread().getName());
    executorA.submit(
        () -> {
          future.complete(Thread.currentThread().getName());
        });
    assertEquals("thread-B-0", future2.get());
  }

  @Test
  public void testOnExecutorNotCompletedYetMain() throws ExecutionException, InterruptedException {
    ExecutorService executorB =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("thread-B-%d").setDaemon(true).build());
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<String> future2 =
        Utils.onExecutor(future, executorB)
            .toCompletableFuture()
            .thenApply(s -> Thread.currentThread().getName());
    future.complete(Thread.currentThread().getName());
    assertEquals("thread-B-0", future2.get());
  }

  @Test
  public void testOnExecutorAlreadyCompleted() throws ExecutionException, InterruptedException {
    ExecutorService executorB =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("thread-B-%d").setDaemon(true).build());

    CompletableFuture<String> future = new CompletableFuture<>();
    future.complete(Thread.currentThread().getName());

    CompletableFuture<String> future2 =
        Utils.onExecutor(future, executorB)
            .toCompletableFuture()
            .thenApply(s -> Thread.currentThread().getName());
    String actual = future2.get();

    // It doesn't matter that we try to use an executor, since the future is already completed.
    // Even if we could run complete the resulting future on the executor, we will still run on
    // the current thread if thenApply() is called after the future has finished.
    assertEquals(Thread.currentThread().getName(), actual);
  }

  @Test
  public void testOnExecutorException() throws ExecutionException, InterruptedException {
    ExecutorService executorB =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("thread-B-%d").setDaemon(true).build());
    CompletableFuture<String> future = new CompletableFuture<>();
    CompletableFuture<String> future2 =
        Utils.onExecutor(future, executorB)
            .toCompletableFuture()
            .exceptionally(s -> Thread.currentThread().getName());
    future.completeExceptionally(new RuntimeException());
    assertEquals("thread-B-0", future2.get());
  }
}
