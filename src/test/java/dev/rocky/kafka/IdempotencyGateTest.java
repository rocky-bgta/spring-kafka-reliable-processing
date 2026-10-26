package dev.rocky.kafka;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class IdempotencyGateTest {
 @Test void duplicateEventIsRejected() {var gate=new IdempotencyGate(); assertTrue(gate.claim("evt-1"));assertFalse(gate.claim("evt-1"));}
 @Test void concurrentDuplicateIsAcceptedExactlyOnce() throws Exception {
  var gate=new IdempotencyGate();var pool=Executors.newFixedThreadPool(8);var start=new CountDownLatch(1);
  try {var futures=new java.util.ArrayList<Future<Boolean>>();for(int i=0;i<20;i++) futures.add(pool.submit(()->{start.await();return gate.claim("same-id");}));start.countDown();int wins=0;for(var f:futures) if(f.get(5,TimeUnit.SECONDS)) wins++;assertEquals(1,wins);} finally {pool.shutdownNow();}
 }
 @Test void blankIdIsRejected(){assertThrows(IllegalArgumentException.class,()->new IdempotencyGate().claim(" "));}
}
