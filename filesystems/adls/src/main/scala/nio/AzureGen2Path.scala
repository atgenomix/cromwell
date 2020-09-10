package nio

import java.io.IOException
import java.net.{URI, URISyntaxException}
import java.nio.file._
import java.util

import com.azure.core.util.logging.ClientLogger

import scala.collection.JavaConverters._

object AzureGen2Path {
  private val logger = new ClientLogger(classOf[AzureGen2Path])

  def apply(parentFileSystem: AzureGen2FileSystem, fileStoreName: String, first: String, more: String*): AzureGen2Path = {
    val separator = AzureGen2FileSystem.PATH_SEPARATOR
    val elements = first.split(separator) ++ more.flatMap(_.split(separator))
    val pathString = elements.filter(_.nonEmpty).mkString(separator)

    new AzureGen2Path(parentFileSystem, fileStoreName, pathString)
  }
}

case class AzureGen2Path(parentFileSystem: AzureGen2FileSystem, fileStoreName: String, pathString: String) extends Path {
  /**
    * Returns the file system that created this object.
    *
    * @return the file system that created this object
    */
  override def getFileSystem: FileSystem = this.parentFileSystem

  /**
    * Tells whether or not this path is absolute.
    * <p>
    * An absolute path is complete in that it doesn't need to be combined with other path information in order to
    * locate a file. A path is considered absolute in this file system if it contains a root component.
    *
    * @return whether the path is absolute
    */
  override def isAbsolute: Boolean = this.getRoot != null

  /**
    * Returns the root component of this path as a Path object, or null if this path does not have a root component.
    * <p>
    * The root component of this path also identifies the Azure Storage Container in which the file is stored. This
    * method will not validate that the root component corresponds to an actual file store/container in this
    * file system. It will simply return the root component of the path if one is present and syntactically valid.
    *
    * @return a path representing the root component of this path, or null
    */
  override def getRoot: Path = this.parentFileSystem.getPathByFileStore(fileStoreName, "")

  /**
    * Returns the name of the file or directory denoted by this path as a Path object. The file name is the farthest
    * element from the root in the directory hierarchy.
    *
    * @return a path representing the name of the file or directory, or null if this path has zero elements
    */
  override def getFileName: Path =
    if (this.isRoot)
      None.orNull
    else if (this.pathString.isEmpty)
      this
    else {
      val elements = this.splitToElements
      this.parentFileSystem.getPathByFileStore(fileStoreName, elements.last)
    }

  /**
    * Returns the parent path, or null if this path does not have a parent.
    * <p>
    * The parent of this path object consists of this path's root component, if any, and each element in the path
    * except for the farthest from the root in the directory hierarchy. This method does not access the file system;
    * the path or its parent may not exist. Furthermore, this method does not eliminate special names such as "." and
    * ".." that may be used in some implementations. On UNIX for example, the parent of "/a/b/c" is "/a/b", and the
    * parent of "x/y/." is "x/y". This method may be used with the normalize method, to eliminate redundant names, for
    * cases where shell-like navigation is required.
    * <p>
    * If this path has one or more elements, and no root component, then this method is equivalent to evaluating the
    * expression:
    *
    * {@code subpath(0, getNameCount()-1);}
    *
    * @return a path representing the path's parent
    */
  override def getParent: Path = {
    /*
     * If this path only has one element or is empty, there is no parent. Note the root is included in the parent, so
     * we don't use getNameCount here.
     */
    val elements = this.splitToElements
    if (elements.length == 1 || elements.length == 0)
      None.orNull
    else
      this.parentFileSystem.getPathByFileStore(fileStoreName, this.pathString.substring(0, this.pathString.lastIndexOf(this.parentFileSystem.getSeparator)))
  }

  /**
    * Returns the number of name elements in the path.
    *
    * @return the number of elements in the path, or 0 if this path only represents a root component
    */
  override def getNameCount: Int = {
    if (this.pathString.isEmpty) return 1
    this.splitToElements(this.withoutRoot).length
  }

  /**
    * Returns a name element of this path as a Path object.
    * <p>
    * The index parameter is the index of the name element to return. The element that is closest to the root in the
    * directory hierarchy has index 0. The element that is farthest from the root has index {@code count-1}.
    *
    * @param index the index of the element
    * @return the name element
    * @throws IllegalArgumentException if index is negative, index is greater than or equal to the number of elements,
    *                                  or this path has zero name elements
    */
  override def getName(index: Int): Path = {
    if (index < 0 || index >= this.getNameCount)
      throw LoggingUtility.logError(AzureGen2Path.logger, new IllegalArgumentException(s"Index $index is out of " + "bounds"))
    // If the path is empty, the only valid option is also an empty path.
    if (this.pathString.isEmpty)
      return this

    this.parentFileSystem.getPathByFileStore(fileStoreName, this.splitToElements(this.withoutRoot)(index))
  }

  /**
    * Returns a relative Path that is a subsequence of the name elements of this path.
    * <p>
    * The beginIndex and endIndex parameters specify the subsequence of name elements. The name that is closest to the
    * root in the directory hierarchy has index 0. The name that is farthest from the root has index {@code count-1}.
    * The returned Path object has the name elements that begin at beginIndex and extend to the element at index
    * {@code endIndex-1}.
    *
    * @param begin the index of the first element, inclusive
    * @param end   the index of the last element, exclusive
    * @return a new Path object that is a subsequence of the name elements in this Path
    */
  override def subpath(begin: Int, end: Int): Path = {
    if (begin < 0 || begin >= this.getNameCount || end <= begin || end > this.getNameCount)
      throw LoggingUtility.logError(AzureGen2Path.logger, new IllegalArgumentException(s"Values of begin: $begin and end: $end are invalid"))

    val subnames = this.splitToElements(this.withoutRoot).tail.dropRight(1)
    this.parentFileSystem.getPathByFileStore(fileStoreName, subnames.mkString(parentFileSystem.getSeparator))
  }

  /**
    * Tests if this path starts with the given path.
    * <p>
    * This path starts with the given path if this path's root component starts with the root component of the given
    * path, and this path starts with the same name elements as the given path. If the given path has more name
    * elements than this path then false is returned.
    * <p>
    * If this path does not have a root component and the given path has a root component then this path does not start
    * with the given path.
    * <p>
    * If the given path is associated with a different FileSystem to this path then false is returned.
    * <p>
    * In this implementation, a root component starts with another root component if the two root components are
    * equivalent strings. In other words, if the files are stored in the same container.
    *
    * @param path the given path
    * @return true if this path starts with the given path; otherwise false
    */
  override def startsWith(path: Path): Boolean = {
    if (!(path.getFileSystem == this.parentFileSystem)) return false
    // An empty path never starts with another path and is never the start of another path.
    if (this.pathString.isEmpty ^ path.asInstanceOf[AzureGen2Path].pathString.isEmpty) return false
    val thisPathElements = this.splitToElements
    val otherPathElements = path.asInstanceOf[AzureGen2Path].splitToElements
    if (otherPathElements.length > thisPathElements.length) return false
    for (i <- otherPathElements.indices) {
      if (!(otherPathElements(i) == thisPathElements(i))) return false
    }
    true
  }

  /**
    * Tests if this path starts with a Path, constructed by converting the given path string, in exactly the manner
    * specified by the startsWith(Path) method.
    *
    * @param path the given path string
    * @return true if this path starts with the given path; otherwise false
    * @throws InvalidPathException If the path string cannot be converted to a Path.
    */
  override def startsWith(path: String): Boolean = this.startsWith(this.parentFileSystem.getPathByFileStore(fileStoreName, path))

  /**
    * Tests if this path ends with the given path.
    * <p>
    * If the given path has N elements, and no root component, and this path has N or more elements, then this path
    * ends with the given path if the last N elements of each path, starting at the element farthest from the root,
    * are equal.
    * <p>
    * If the given path has a root component then this path ends with the given path if the root component of this path
    * ends with the root component of the given path, and the corresponding elements of both paths are equal. If this
    * path does not have a root component and the given path has a root component then this path does not end with the
    * given path.
    * <p>
    * If the given path is associated with a different FileSystem to this path then false is returned.
    * <p>
    * In this implementation, a root component ends with another root component if the two root components are
    * equivalent strings. In other words, if the files are stored in the same container.
    *
    * @param path the given path
    * @return true if this path ends with the given path; otherwise false
    */
  override def endsWith(path: Path): Boolean = {
    /*
     #      There can only be one instance of a file system with a given id, so comparing object identity is equivalent
     #      to checking ids here.
     */
    if (path.getFileSystem ne this.parentFileSystem) return false
    // An empty path never ends with another path and is never the end of another path.
    if (this.pathString.isEmpty ^ path.asInstanceOf[AzureGen2Path].pathString.isEmpty) return false
    val thisPathElements = this.splitToElements
    val otherPathElements = path.asInstanceOf[AzureGen2Path].splitToElements
    if (otherPathElements.length > thisPathElements.length) return false
    // If the given path has a root component, the paths must be equal.
    if (path.getRoot != null && otherPathElements.length != thisPathElements.length) return false
    for (i <- 1 to otherPathElements.length) {
      if (!(otherPathElements(otherPathElements.length - i) == thisPathElements(thisPathElements.length - i))) return false
    }
    true
  }

  /**
    * Tests if this path ends with a Path, constructed by converting the given path string, in exactly the manner
    * specified by the endsWith(Path) method.
    *
    * @param path the given path string
    * @return true if this path starts with the given path; otherwise false
    * @throws InvalidPathException If the path string cannot be converted to a Path.
    */
  override def endsWith(path: String): Boolean = this.endsWith(this.parentFileSystem.getPathByFileStore(fileStoreName, path))

  /**
    * Returns a path that is this path with redundant name elements eliminated.
    * <p>
    * It derives from this path, a path that does not contain redundant name elements. The "." and ".." are special
    * names used to indicate the current directory and parent directory. All occurrences of "." are considered
    * redundant. If a ".." is preceded by a non-".." name then both names are considered redundant (the process to
    * identify such names is repeated until is it no longer applicable).
    * <p>
    * This method does not access the file system; the path may not locate a file that exists. Eliminating ".." and a
    * preceding name from a path may result in the path that locates a different file than the original path
    *
    * @return the resulting path or this path if it does not contain redundant name elements; an empty path is returned
    *         if this path does have a root component and all name elements are redundant
    *
    */
  override def normalize(): Path = {
    val stack = new util.ArrayDeque[String]
    val pathElements = this.splitToElements
    val root = this.getRoot
    val rootStr = if (root == null) null else root.toString

    pathElements.foreach { element =>
      if (element == ".") {
        ()
      }
      // Root path. We never push "..".
      // Cannot go higher than root. Ignore.
      else if (element == ".." && (stack.isEmpty || stack.peekLast() != rootStr)) {
        stack.removeLast()
      }
      else {
        if (stack.isEmpty) stack.addLast(element)
        else if (stack.peek == "..") stack.addLast(element)
        else stack.removeLast()
      }
    }
    this.parentFileSystem.getPathByFileStore(fileStoreName, "", stack.toArray(new Array[String](0)): _*)
  }

  /**
    * Resolve the given path against this path.
    * <p>
    * If the other parameter is an absolute path then this method trivially returns other. If other is an empty path
    * then this method trivially returns this path. Otherwise this method considers this path to be a directory and
    * resolves the given path against this path. In the simplest case, the given path does not have a root component,
    * in which case this method joins the given path to this path and returns a resulting path that ends with the given
    * path. Where the given path has a root component then resolution is highly implementation dependent and therefore
    * unspecified.
    *
    * @param path the path to resolve against this path
    * @return the resulting path
    */
  override def resolve(path: Path): Path = {
    if (path.isAbsolute) return path
    if (path.getNameCount == 0) return this
    this.parentFileSystem.getPathByFileStore(fileStoreName, this.toString, path.toString)
  }

  /**
    * Converts a given path string to a Path and resolves it against this Path in exactly the manner specified by the
    * {@link #resolve(Path) resolve} method.
    *
    * @param path the path string to resolve against this path
    * @return the resulting path
    * @throws InvalidPathException if the path string cannot be converted to a Path.
    */
  override def resolve(path: String): Path = this.resolve(this.parentFileSystem.getPathByFileStore(fileStoreName, path))

  /**
    * Resolves the given path against this path's parent path. This is useful where a file name needs to be replaced
    * with another file name. For example, suppose that the name separator is "/" and a path represents
    * "dir1/dir2/foo", then invoking this method with the Path "bar" will result in the Path "dir1/dir2/bar". If this
    * path does not have a parent path, or other is absolute, then this method returns other. If other is an empty path
    * then this method returns this path's parent, or where this path doesn't have a parent, the empty path.
    *
    * @param path the path to resolve against this path's parent
    * @return the resulting path
    */
  override def resolveSibling(path: Path): Path = {
    if (path.isAbsolute) return path
    val parent = this.getParent
    if (parent == null) path
    else parent.resolve(path)
  }

  /**
    * Converts a given path string to a Path and resolves it against this path's parent path in exactly the manner
    * specified by the resolveSibling method.
    *
    * @param path the path string to resolve against this path's parent
    * @return the resulting path
    * @throws InvalidPathException if the path string cannot be converted to a Path.
    */
  override def resolveSibling(path: String): Path = this.resolveSibling(this.parentFileSystem.getPathByFileStore(fileStoreName, path))

  /**
    * Constructs a relative path between this path and a given path.
    * <p>
    * Relativization is the inverse of resolution. This method attempts to construct a relative path that when resolved
    * against this path, yields a path that locates the same file as the given path.
    * <p>
    * A relative path cannot be constructed if only one of the paths have a root component. If both paths have a root
    * component, it is still possible to relativize one against the other. If this path and the given path are equal
    * then an empty path is returned.
    * <p>
    * For any two normalized paths p and q, where q does not have a root component,
    * {@code p.relativize(p.resolve(q)).equals(q)}
    *
    * @param path the path to relativize against this path
    * @return the resulting relative path, or an empty path if both paths are equal
    * @throws IllegalArgumentException if other is not a Path that can be relativized against this path
    */
  override def relativize(path: Path): Path = {
    if (path.getRoot == null ^ this.getRoot == null)
      throw LoggingUtility.logError(AzureGen2Path.logger, new IllegalArgumentException("Both paths must be absolute or neither can be"))

    val thisNormalized = this.normalize().asInstanceOf[AzureGen2Path]
    val otherNormalized = path.normalize()
    val deque = new util.ArrayDeque[String](otherNormalized.toString.split(this.parentFileSystem.getSeparator).toList.asJava)
    var i = 0
    val thisElements = thisNormalized.splitToElements
    while ( {
      i < thisElements.length && !deque.isEmpty && thisElements(i) == deque.peekFirst
    }) {
      deque.removeFirst()
      i += 1
    }
    while ( {
      i < thisElements.length
    }) {
      deque.addFirst("..")
      i += 1
    }
    this.parentFileSystem.getPathByFileStore(fileStoreName, "", deque.toArray(new Array[String](0)).toList: _*)
  }

  /**
    * Returns a URI to represent this path.
    * <p>
    * This method constructs an absolute URI with a scheme equal to the URI scheme that identifies the provider.
    * <p>
    * No authority component is defined for the {@code URI} returned by this method. This implementation offers the
    * same equivalence guarantee as the default provider.
    *
    * @return the URI representing this path
    * @throws SecurityException never
    */
  override def toUri: URI = try new URI(this.parentFileSystem.provider.getScheme, null, "/" + this.toAbsolutePath.toString, null, null)
  catch {
    case e: URISyntaxException =>
      throw LoggingUtility.logError(AzureGen2Path.logger, new IllegalStateException("Unable to create valid URI from path", e))
  }

  /**
    * Returns a Path object representing the absolute path of this path.
    * <p>
    * If this path is already absolute then this method simply returns this path. Otherwise, this method resolves the
    * path against the default directory.
    *
    * @return a Path object representing the absolute path
    * @throws SecurityException never
    */
  override def toAbsolutePath: Path = {
    this
  }

  /**
    * Unsupported.
    * <p>
    *
    * @param linkOptions options
    * @return the real path
    * @throws UnsupportedOperationException operation not suported.
    */
  @throws[IOException]
  override def toRealPath(linkOptions: LinkOption*) = throw new UnsupportedOperationException("Symbolic links are not supported.")

  /**
    * Unsupported.
    * <p>
    *
    * @return the file
    * @throws UnsupportedOperationException operation not suported.
    */
  override def toFile = throw new UnsupportedOperationException

  /**
    * Unsupported.
    * <p>
    *
    * @param watchService watchService
    * @param kinds        kinds
    * @param modifiers    modifiers
    * @return the watch key
    * @throws UnsupportedOperationException operation not suported.
    */
  @throws[IOException]
  override def register(watchService: WatchService, kinds: Array[WatchEvent.Kind[_]], modifiers: WatchEvent.Modifier*) = throw new UnsupportedOperationException("WatchEvents are not supported.")

  /**
    * Unsupported.
    * <p>
    *
    * @param watchService watchService
    * @param kinds        kinds
    * @return the watch key
    * @throws UnsupportedOperationException operation not suported.
    */
  @throws[IOException]
  override def register(watchService: WatchService, kinds: WatchEvent.Kind[_]*) = throw new UnsupportedOperationException("WatchEvents are not supported.")

  /**
    * Returns an iterator over the name elements of this path.
    * <p>
    * The first element returned by the iterator represents the name element that is closest to the root in the
    * directory hierarchy, the second element is the next closest, and so on. The last element returned is the name of
    * the file or directory denoted by this path. The root component, if present, is not returned by the iterator.
    *
    * @return an iterator over the name elements of this path.
    */
  override def iterator: util.Iterator[Path] = {
    if (this.pathString.isEmpty)
      Iterator[Path]().asJava

    this.splitToElements(this.withoutRoot)
      .map(s => this.parentFileSystem.getPathByFileStore(fileStoreName, s).asInstanceOf[Path])
      .toIterator
      .asJava
  }

  /**
    * Compares two abstract paths lexicographically. This method does not access the file system and neither file is
    * required to exist.
    * <p>
    * This method may not be used to compare paths that are associated with different file system providers.
    * <p>
    * This result of this method is identical to a string comparison on the underlying path strings.
    *
    * @return zero if the argument is equal to this path, a value less than zero if this path is lexicographically less
    *         than the argument, or a value greater than zero if this path is lexicographically greater than the argument
    * @throws ClassCastException if the paths are associated with different providers
    */
  override def compareTo(path: Path): Int = {
    if (!path.isInstanceOf[AzureGen2Path])
      throw LoggingUtility.logError(AzureGen2Path.logger, new ClassCastException("Other path is not an instance of " + "AzureGen2Path."))

    this.pathString.compareTo(path.asInstanceOf[AzureGen2Path].pathString)
  }

  /**
    * Returns the string representation of this path.
    * <p>
    * If this path was created by converting a path string using the getPath method then the path string returned by
    * this method may differ from the original String used to create the path.
    * <p>
    * The returned path string uses the default name separator to separate names in the path.
    *
    * @return the string representation of this path
    */
  override def toString: String = this.pathString

  /**
    * A path is considered equal to another path if it is associated with the same file system instance and if the
    * path strings are equivalent.
    *
    * @return true if, and only if, the given object is a Path that is identical to this Path
    */
  override def equals(that: Any): Boolean = {
    that match {
      case that: AzureGen2Path => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  }

  override def hashCode: Int = (parentFileSystem.getFileSystemName + pathString).hashCode

  /*
  We don't store the file client because unlike other types in this package, a Path does not actually indicate the
  existence or even validity of any remote resource. It is purely a representation of a path. Therefore, we do not
  construct the client or perform any validation until it is requested.
   */
  @throws[IOException]
  private[nio] def toFileClient = {
    val fileStoreClient = this.parentFileSystem.getFileStore(fileStoreName).asInstanceOf[AzureGen2FileStore].getFileStoreClient
    val blobName = this.withoutRoot
    if (blobName.isEmpty)
      throw new IOException("Cannot get a blob client to a path that only contains the root or is an empty path")

    if (blobName.endsWith("/"))
      throw new IOException("Cannot get a file client to a dir path")

    fileStoreClient.getFileClient(blobName)
  }

  @throws[IOException]
  private[nio] def toDirectoryClient = {
    val fileStoreClient = this.parentFileSystem.getFileStore(fileStoreName).asInstanceOf[AzureGen2FileStore].getFileStoreClient
    val blobName = this.withoutRoot
    if (blobName.isEmpty)
      throw new IOException("Cannot get a blob client to a path that only contains the root or is an empty path")

    if (!blobName.endsWith("/"))
      throw new IOException("Cannot get a dir client to a file path")

    fileStoreClient.getDirectoryClient(blobName)
  }

  @throws[IOException]
  private[nio] def toFileSystemClient = { // Converting to an absolute path ensures there is a container to operate on even if it is the default.
    // Normalizing ensures the path is clean.
    val root = this.normalize().toAbsolutePath.getRoot
    if (root == null)
      throw LoggingUtility.logError(AzureGen2Path.logger, new IllegalStateException("Root should never be null after calling toAbsolutePath."))

    this.parentFileSystem.getFileStore(fileStoreName).asInstanceOf[AzureGen2FileStore].getFileStoreClient
  }

  /**
    * @return Whether this path consists of only a root component.
    */
  private[nio] def isRoot = this == this.getRoot

  private[nio] def withoutRoot: String = {
    if (this.pathString.startsWith("/"))
      this.pathString.substring(1)
    else
      this.pathString
  }

  private def splitToElements: Array[String] = this.splitToElements(this.pathString)

  private def splitToElements(str: String): Array[String] = {
    val arr = str.split(this.parentFileSystem.getSeparator).filter(_.nonEmpty)
    /*
     * This is a special case where we split after removing the root from a path that is just the root. Or otherwise
     * have an empty path.
     */
    if (arr.length == 1 && arr(0).isEmpty) return new Array[String](0)

    arr
  }
}
