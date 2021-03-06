package cromwell.engine.io.nio

import java.nio.file.{FileAlreadyExistsException, NoSuchFileException}
import java.util.UUID

import akka.actor.ActorRef
import akka.stream.scaladsl.{Keep, Sink, Source}
import cats.effect.IO
import com.google.cloud.storage.StorageException
import cromwell.core.io.DefaultIoCommandBuilder._
import cromwell.core.io._
import cromwell.core.path.DefaultPathBuilder
import cromwell.core.{CromwellFatalExceptionMarker, TestKitSuite}
import cromwell.engine.io.IoActor.DefaultCommandContext
import cromwell.engine.io.IoCommandContext
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class NioFlowSpec extends TestKitSuite with AsyncFlatSpecLike with Matchers with MockitoSugar {

  behavior of "NioFlowSpec"

  val flow = new NioFlow(1, system.scheduler)(system.dispatcher).flow

  val replyTo = mock[ActorRef]
  val readSink = Sink.head[(IoAck[_], IoCommandContext[_])]

  override def afterAll() = {
    super.afterAll()
  }

  it should "write to a Nio Path" in {
    val testPath = DefaultPathBuilder.createTempFile()
    val context = DefaultCommandContext(writeCommand(testPath, "hello", Seq.empty), replyTo)
    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)

    stream.run() map { _ =>
      assert(testPath.contentAsString == "hello")
    }
  }

  it should "read from a Nio Path" in {
    val testPath = DefaultPathBuilder.createTempFile()
    testPath.write("hello")
    
    val context = DefaultCommandContext(contentAsStringCommand(testPath, None, failOnOverflow = false), replyTo)
    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)
    
    stream.run() map {
      case (success: IoSuccess[_], _) => assert(success.result.asInstanceOf[String] == "hello")
      case _ => fail("read returned an unexpected message")
    }
  }

  it should "get size from a Nio Path" in {
    val testPath = DefaultPathBuilder.createTempFile()
    testPath.write("hello")

    val context = DefaultCommandContext(sizeCommand(testPath), replyTo)
    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)

    stream.run() map {
      case (success: IoSuccess[_], _) => assert(success.result.asInstanceOf[Long] == 5)
      case _ => fail("size returned an unexpected message")
    }
  }
  
  it should "get hash from a Nio Path" in {
    val testPath = DefaultPathBuilder.createTempFile()
    testPath.write("hello")

    val context = DefaultCommandContext(hashCommand(testPath), replyTo)
    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)

    stream.run() map {
      case (success: IoSuccess[_], _) => assert(success.result.asInstanceOf[String] == "5d41402abc4b2a76b9719d911017c592")
      case _ => fail("hash returned an unexpected message")
    }
  }

  it should "copy Nio paths" in {
    val testPath = DefaultPathBuilder.createTempFile()
    val testCopyPath = testPath.sibling(UUID.randomUUID().toString)

    val context = DefaultCommandContext(copyCommand(testPath, testCopyPath, overwrite = false), replyTo)

    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)

    stream.run() map {
      case (_: IoSuccess[_], _) => assert(testCopyPath.exists)
      case _ => fail("copy returned an unexpected message")
    }
  }

  it should "copy Nio paths with overwrite true" in {
    val testPath = DefaultPathBuilder.createTempFile()
    testPath.write("goodbye")
    
    val testCopyPath = DefaultPathBuilder.createTempFile()
    testCopyPath.write("hello")
    
    val context = DefaultCommandContext(copyCommand(testPath, testCopyPath, overwrite = true), replyTo)

    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)

    stream.run() map {
      case (_: IoSuccess[_], _) =>
        assert(testCopyPath.exists)
        assert(testCopyPath.contentAsString == "goodbye")
      case _ => fail("copy returned an unexpected message")
    }
  }

  it should "copy Nio paths with overwrite false" in {
    val testPath = DefaultPathBuilder.createTempFile()
    val testCopyPath = DefaultPathBuilder.createTempFile()

    val context = DefaultCommandContext(copyCommand(testPath, testCopyPath, overwrite = false), replyTo)

    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)

   stream.run() map {
      case (failure: IoFailure[_], _) =>
        assert(failure.failure.isInstanceOf[CromwellFatalExceptionMarker])
        assert(failure.failure.getCause.isInstanceOf[FileAlreadyExistsException])
      case _ => fail("copy returned an unexpected message")
    }
  }

  it should "delete a Nio path" in {
    val testPath = DefaultPathBuilder.createTempFile()
    val context = DefaultCommandContext(deleteCommand(testPath, swallowIoExceptions = false), replyTo)
    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)

    stream.run() map {
      case (_: IoSuccess[_], _) => assert(!testPath.exists)
      case _ => fail("delete returned an unexpected message")
    }
  }

  it should "delete a Nio path with swallowIoExceptions true" in {
    val testPath = DefaultPathBuilder.build("/this/does/not/exist").get

    val context = DefaultCommandContext(deleteCommand(testPath, swallowIoExceptions = true), replyTo)

    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)

    stream.run() map {
      case (_: IoSuccess[_], _) => assert(!testPath.exists)
      case _ => fail("delete returned an unexpected message")
    }
  }

  it should "delete a Nio path with swallowIoExceptions false" in {
    val testPath = DefaultPathBuilder.build("/this/does/not/exist").get

    val context = DefaultCommandContext(deleteCommand(testPath, swallowIoExceptions = false), replyTo)

    val testSource = Source.single(context)

    val stream = testSource.via(flow).toMat(readSink)(Keep.right)

    stream.run() map {
      case (failure: IoFailure[_], _) =>
        assert(failure.failure.isInstanceOf[CromwellFatalExceptionMarker])
        assert(failure.failure.getMessage == "[Attempted 1 time(s)] - NoSuchFileException: /this/does/not/exist")
        assert(failure.failure.getCause.isInstanceOf[NoSuchFileException])
      case other @ _ => fail(s"delete returned an unexpected message")
    }
  }

  it should "retry on retryable exceptions" in {
    val testPath = DefaultPathBuilder.build("does/not/matter").get

    val context = DefaultCommandContext(contentAsStringCommand(testPath, None, failOnOverflow = false), replyTo)

    val testSource = Source.single(context)

    val customFlow = new NioFlow(1, system.scheduler, NioFlow.NoopOnRetry, 3)(system.dispatcher) {
      private var tries = 0
      override def handleSingleCommand(ioSingleCommand: IoCommand[_]) = {
        IO {
          tries += 1
          if (tries < 3) throw new StorageException(500, "message")
          else IoSuccess(ioSingleCommand, "content")
        }
       }
    }.flow
    
    val stream = testSource.via(customFlow).toMat(readSink)(Keep.right)

    stream.run() map {
      case (success: IoSuccess[_], _) => assert(success.result.asInstanceOf[String] == "content")
      case _ => fail("read returned an unexpected message")
    }
  }

}
