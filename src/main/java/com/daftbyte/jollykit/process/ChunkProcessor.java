package com.daftbyte.jollykit.process;

import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;


/**
 * Class provides support to process large datasets chunked.<br/>
 *
 * @param <K>
 *           The key type of the id that we want to select, for example Long, if we select entities with long ids
 * @param <T>
 *           Type of the input entities we process
 * @param <R>
 *           Type of the result entities or classes we produce from the L types
 * 
 * @author Marton Szabo jollyblade@gmail.com
 */
public abstract class ChunkProcessor<K, T, R> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private static final int DEFAULT_CHUNK_SIZE = 500;

	protected final int chunkSize;

	protected Date now;

	protected ErrorReport errors;

	/**
     * Name of the processor class, usually the name of the class, can be changed in child constructor
     */
	protected String name;

	protected ChunkProcessor() {
		this(DEFAULT_CHUNK_SIZE);
	}

	protected ChunkProcessor(int chunkSize) {
        this.chunkSize = chunkSize;
        this.name = getClass().getSimpleName();
        this.now = new Date();
	}

	protected ChunkProcessor(String name, int chunkSize) {
        this.name = name;
		this.chunkSize = chunkSize;
        this.now = new Date();
	}

    protected ChunkProcessor(String name) {
        this(name, DEFAULT_CHUNK_SIZE);
    }

	/**
	 * Method provides a template to process large datasets chunked, by expecting a list of ids, splitting them to smaller chunks,
	 * retrieving the data with the sub id lists, and then processing the small lists one after another The method is
	 * aggregating the counts from the mappers.
	 *
	 * @param chunkSize
	 *           the size of the small lists, 100 means we go by hundreds of records at once
	 * @param idSupplier
	 *           this is simply returning the complete list of ids to split
	 * @param listSelector
	 *           list selector takes a small (chunksize) list of ids, and uses the provided function to retrieve tha data, can be a query for example,
     *           using the small list as input, and returns a list of entities to process
	 * @param mapper
	 *           processes one entity, creates another one from that
	 *
	 * @return the total count of created entities
	 *
	 * @throws Exception
	 */
	private int processChunked(final int chunkSize, final Supplier<List<K>> idSupplier,
			final Function<List<K>, List<T>> listSelector, final Function<T, R> mapper) throws Exception {

		final int[] processed = new int[1];

		final List<K> keys = idSupplier.get();
		final List<List<K>> chunks = Lists.partition(keys, chunkSize);

		chunks.stream().map(listSelector).forEach((list) -> {
			// count is the terminal operation, so it must be called to have the data processed
			beforeChunk();

            long count = list.stream().map(mapper).count();

            afterChunk();

            processed[0] += count;
            logProcessChunkFinished((int) count);
        });

		return processed[0];
	}

	/**
	 * Processes data chunked, gets the list of ids, splits it up to sublists size defined by getChunkSize, and processes the records
	 * Logs outcome, info and errors.
	 *
	 * @return number of records processed/created depending on process return value
	 */
	public int process() {
        errors = new ErrorReport("Error report " + name);
		int recordsProcessed = 0;

		try {
			logProcessStart();
			preprocess();

			recordsProcessed = processChunked(chunkSize,
                    () -> getIds(),
                    (idList) -> getResultsWithIds(idList),
					(ltb) -> processRecord(ltb));

			postprocess();

			logProcessEnd();
		}
		catch (final Exception e) {
			log.error("Could not chunkprocess " + name + ", error: " + e.getMessage(), e);
			logProcessFailed(e);
		}

		if (errors.hasErrors()) {
			log.error("Errors in " + name + ", " + errors.toString());
		}

		return recordsProcessed;
	}

	/**
	 * Initialize additional data
	 *
	 * @throws Exception
	 */
	protected void preprocess() throws Exception {
		// TODO override to initialize custom data
	}

	/**
	 * Initialize additional data
	 *
	 * @throws Exception
	 */
	protected void postprocess() throws Exception {
		// TODO override to tear down custom data
	}

	/**
	 * Return the list of ids to process
	 * 
	 * @return
	 */
	protected abstract List<K> getIds();

	/**
	 * Selects the list of data to process based on the sublist of ids
	 * 
	 * @param idLIst
	 * @return
	 */
	protected abstract List<T> getResultsWithIds(List<K> idLIst);

	/**
	 * Processes one single record
	 * 
	 * @param data
	 * @return
	 */
	protected abstract R processRecord(T data);

    protected abstract void beforeChunk();

    protected abstract void afterChunk();

    /**
     * Override this method to do custom logging
     */
	protected void logProcessStart() {
		log.debug("Started {}", name);
	}

    /**
     * Override this method to do custom logging
     */
	protected void logProcessEnd() {
		log.debug("Process ended {}", name);
	}

    /**
     * Override this method to do custom logging
     */
	protected void logProcessChunkFinished(final int count) {
        log.debug("{} processed {} records", name, count);
	}

    /**
     * Override this method to do custom logging
     */
	protected void logProcessFailed(final Exception e) {
		log.debug("{} process failed: {}", name, e);
	}
}
