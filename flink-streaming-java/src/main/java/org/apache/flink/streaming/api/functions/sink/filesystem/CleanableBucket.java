package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * An interface to be used by the {@link BucketOutdatedParts}
 * to remove outdated parts.
 */
@Internal
public interface CleanableBucket {

	/**
	 * Removes outdated parts from bucket.
	 * @throws IOException Thrown, if any I/O related problem occurred
	 */
	void clean() throws IOException;
}
