package com.nsn.ngdb.hive.udf;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.KeyBuffer;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * CustomRCFileRecordReader.
 * 
 * @param <K>
 * @param <V>
 */
public class CustomRCFileRecordReader<K extends LongWritable, V extends BytesRefArrayWritable>
		implements RecordReader<LongWritable, BytesRefArrayWritable> {

	private final Reader in;
	private final long start;
	private final long end;
	private boolean more = true;
	protected Configuration conf;
	private final FileSplit split;
	private final boolean useCache;

	private static RCFileSyncCache syncCache = new RCFileSyncCache();

	private static final class RCFileSyncEntry {
		long end;
		long endSync;
	}

	private static final class RCFileSyncCache {

		private final Map<String, RCFileSyncEntry> cache;

		public RCFileSyncCache() {
			cache = Collections
					.synchronizedMap(new WeakHashMap<String, RCFileSyncEntry>());
		}

		public void put(FileSplit split, long endSync) {
			Path path = split.getPath();
			long end = split.getStart() + split.getLength();
			String key = path.toString() + "+" + String.format("%d", end);

			RCFileSyncEntry entry = new RCFileSyncEntry();
			entry.end = end;
			entry.endSync = endSync;
			if (entry.endSync >= entry.end) {
				cache.put(key, entry);
			}
		}

		public long get(FileSplit split) {
			Path path = split.getPath();
			long start = split.getStart();
			String key = path.toString() + "+" + String.format("%d", start);
			RCFileSyncEntry entry = cache.get(key);
			if (entry != null) {
				return entry.endSync;
			}
			return -1;
		}
	}

	public CustomRCFileRecordReader(Configuration conf, long offset,long length,Path inputPath)
			throws IOException {

		Path path = inputPath;
		FileSystem fs = path.getFileSystem(conf);
		this.in = new RCFile.Reader(fs, path, conf);
		this.end = offset + length;
		this.conf = conf;
		this.split = null;

		useCache = HiveConf.getBoolVar(conf,
				HiveConf.ConfVars.HIVEUSERCFILESYNCCACHE);

		if (offset > in.getPosition()) {
			long oldSync = useCache ? syncCache.get(split) : -1;
			if (oldSync == -1) {
				in.sync(offset); // sync to start
			} else {
				in.seek(oldSync);
			}
		}

		this.start = in.getPosition();

		more = start < end;
	}

	public Class<?> getKeyClass() {
		return LongWritable.class;
	}

	public Class<?> getValueClass() {
		return BytesRefArrayWritable.class;
	}

	public LongWritable createKey() {
		return (LongWritable) ReflectionUtils.newInstance(getKeyClass(), conf);
	}

	public BytesRefArrayWritable createValue() {
		return (BytesRefArrayWritable) ReflectionUtils.newInstance(
				getValueClass(), conf);
	}

	public boolean nextBlock() throws IOException {
		return in.nextBlock();
	}

	@Override
	public boolean next(LongWritable key, BytesRefArrayWritable value)
			throws IOException {

		more = next(key);

		if (more) {
			in.getCurrentRow(value);
		}
		return more;
	}

	protected boolean next(LongWritable key) throws IOException {
		if (!more) {
			return false;
		}

		more = in.next(key);

		long lastSeenSyncPos = in.lastSeenSyncPos();

		if (lastSeenSyncPos >= end) {
			if (useCache) {
				syncCache.put(split, lastSeenSyncPos);
			}
			more = false;
			return more;
		}
		return more;
	}

	/**
	 * Return the progress within the input split.
	 * 
	 * @return 0.0 to 1.0 of the input byte range
	 */
	public float getProgress() throws IOException {
		if (end == start) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (in.getPosition() - start)
					/ (float) (end - start));
		}
	}

	public long getPos() throws IOException {
		return in.getPosition();
	}

	public KeyBuffer getKeyBuffer() {
		return in.getCurrentKeyBufferObj();
	}

	protected void seek(long pos) throws IOException {
		in.seek(pos);
	}

	public void sync(long pos) throws IOException {
		in.sync(pos);
	}

	public void resetBuffer() {
		in.resetBuffer();
	}

	public long getStart() {
		return start;
	}

	public void close() throws IOException {
		in.close();
	}
}

