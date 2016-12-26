package net.sf.jabref.fulltext.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import com.google.common.eventbus.Subscribe;
import net.sf.jabref.Globals;
import net.sf.jabref.JabRefException;
import net.sf.jabref.JabRefExecutorService;
import net.sf.jabref.gui.worker.Worker;
import net.sf.jabref.model.database.BibDatabaseContext;
import net.sf.jabref.model.database.event.EntryAddedEvent;
import net.sf.jabref.model.database.event.EntryRemovedEvent;
import net.sf.jabref.model.entry.BibEntry;
import net.sf.jabref.model.entry.event.FieldChangedEvent;
import net.sf.jabref.model.metadata.event.MetaDataChangedEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static net.sf.jabref.fulltext.indexing.LuceneTools.Status.CLOSED;
import static net.sf.jabref.fulltext.indexing.LuceneTools.Status.NOT_IN_USE;
import static net.sf.jabref.fulltext.indexing.LuceneTools.Status.SEARCHABLE;
import static net.sf.jabref.fulltext.indexing.LuceneTools.Status.WRITING;
import static net.sf.jabref.logic.util.io.FileUtil.getListOfLinkedFiles;


public class FullTextIndexer implements Worker {

    private static final long PERIOD = 5000L;

    private static final Log LOGGER = LogFactory.getLog(FullTextIndexer.class);
    private final BibDatabaseContext databaseContext;
    private final LuceneTools lt;
    private Set<LuceneID> toUpdate;
    private ScheduledFuture<?> futureTask;


    public FullTextIndexer(final BibDatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
        lt = new LuceneTools();
    }

    @Override
    public void run() {
        if( toUpdate.isEmpty() ) return;

        try {
            lt.openWriter();
            /*int res = JOptionPane.showConfirmDialog(null, "There are " + toUpdate.size() + " files to reindex. Do you want to do it now?", "Reindex", JOptionPane.YES_NO_OPTION);
            if( res == JOptionPane.YES_OPTION ) {*/
            LOGGER.info("Re-indexing.");
            toUpdate.forEach(d -> {
                lt.updateLuceneDocument(d);
                toUpdate.remove(d);
            });
            //}
            lt.closeWriter();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    public void setup() throws JabRefException {
        LOGGER.debug("Setting up indexer");
        final Optional<Path> dbPath = databaseContext.getDatabasePath();

        if( !dbPath.isPresent() ) throw new JabRefException("cannot run indexer on unsaved database");

        Path indexDir = Paths.get(dbPath.get() + ".lucene");
        try {
            lt.setDirectory(indexDir);
            if( lt.isEmpty() ) {
                LOGGER.warn("Index is still empty");
                lt.createEmptyIndex();
            }
            lt.open();
            toUpdate = isNotUpToDate();

            futureTask = JabRefExecutorService.INSTANCE.scheduleWithFixedDelay(this, 100L, PERIOD);

        } catch (final IOException e) {
            throw new JabRefException("Exception opening/creating the fulltext index", e);
        }

        databaseContext.getDatabase()
                .registerListener(this);
        databaseContext.getMetaData()
                .registerListener(this);
    }


    public void tearDown() throws JabRefException {
        if( lt.getStatus() != CLOSED ) throw new JabRefException("Close indexer before destroying it");

        LOGGER.debug("Destroying DB");
        databaseContext.getDatabase()
                .unregisterListener(this);

        futureTask.cancel(true);

        lt.setUnused();
    }


    @Subscribe
    public void listen(final FieldChangedEvent fieldChangedEvent) {
        final String oldValue = fieldChangedEvent.getOldValue();
        final String newValue = fieldChangedEvent.getNewValue();
        if( "file".equals(fieldChangedEvent.getFieldName()) ) {
            LOGGER.debug("file change: recompute");
            LOGGER.debug(oldValue + " to " + newValue);

            fieldChangedEvent.getBibEntry()
                    .getCiteKeyOptional()
                    .ifPresent(key -> {

                        final Set<File> oldFiles = new HashSet<>(getListOfLinkedFiles(oldValue, databaseContext.getFileDirectories(Globals.prefs.getFileDirectoryPreferences())));
                        final Set<File> newFiles = new HashSet<>(getListOfLinkedFiles(newValue, databaseContext.getFileDirectories(Globals.prefs.getFileDirectoryPreferences())));

                        final Set<File> added = new HashSet<>(newFiles);
                        added.removeAll(oldFiles);

                        final Set<File> removed = new HashSet<>(oldFiles);
                        removed.removeAll(newFiles);

                        try {
                            LOGGER.info("update in lucene, file " + oldValue + " to " + newValue);
                            switch( lt.getStatus() ) {
                                case SEARCHABLE:
                                case CLOSED:
                                    lt.openWriter();
                                    for( final File f : added ) lt.indexFile(new LuceneID(f, key));
                                    for( final File f : removed ) lt.removeFile(new LuceneID(f, key));
                                    lt.closeWriter();
                                    break;
                                case WRITING:
                                    lt.changeEntryKey(oldValue, newValue);
                                    break;
                                case NOT_IN_USE:
                                    LOGGER.warn("Not in use: should not be listening");
                                    break;
                            }
                        } catch (final Exception e) {
                            e.printStackTrace();
                        }
                    });
        }
        if( fieldChangedEvent.getFieldName()
                .equals(BibEntry.KEY_FIELD) ) {
            try {
                LOGGER.info("update in lucene, changing ID from " + oldValue + " to " + newValue);
                switch( lt.getStatus() ) {
                    case SEARCHABLE:
                    case CLOSED:
                        lt.openWriter();
                        lt.changeEntryKey(oldValue, newValue);
                        lt.closeWriter();
                        break;
                    case WRITING:
                        lt.changeEntryKey(oldValue, newValue);
                        break;
                    case NOT_IN_USE:
                        LOGGER.warn("Not in use: should not be listening");
                        break;
                }
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Subscribe
    public void listen(final EntryRemovedEvent entryRemovedEvent) {
        final BibEntry entry = entryRemovedEvent.getBibEntry();
        try {
            switch( lt.getStatus() ) {
                case SEARCHABLE:
                case CLOSED:
                    lt.openWriter();
                    removeEntry(entry);
                    lt.closeWriter();
                    break;
                case WRITING:
                    removeEntry(entry);
                    break;
                case NOT_IN_USE:
                    LOGGER.warn("Not in use: should not be listening");
                    break;
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    @Subscribe
    public void listen(final EntryAddedEvent entryAddedEvent) {
        final BibEntry entry = entryAddedEvent.getBibEntry();
        try {
            switch( lt.getStatus() ) {
                case SEARCHABLE:
                case CLOSED:
                    lt.openWriter();
                    insertEntry(entry);
                    lt.closeWriter();
                    break;
                case WRITING:
                    insertEntry(entry);
                    break;
                case NOT_IN_USE:
                    LOGGER.warn("Not in use: should not be listening");
                    break;
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }


    @Subscribe
    public void listen(final MetaDataChangedEvent event) {
        LOGGER.debug(lt.getStatus());
        try {
            if( lt.getStatus() != NOT_IN_USE && !event.getMetaData()
                    .isFullTextIndexed() ) {
                LOGGER.debug("Time to destroy the world");
                lt.close();
                tearDown();
            } else if( lt.getStatus() == NOT_IN_USE && event.getMetaData()
                    .isFullTextIndexed() ) {
                LOGGER.debug("Going live");
                setup();
                lt.open();
            }
        } catch (final JabRefException e) {
            LOGGER.error(e.getLocalizedMessage());
        }
    }



    public void recreateIndex() throws JabRefException {
        lt.openWriter();

        try {
            LOGGER.debug("recreating. Deleting everything");
            lt.empty();
            for( final BibEntry entry : databaseContext.getDatabase()
                    .getEntries() ) {
                insertEntry(entry);
            }

            lt.closeWriter();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }


    private void insertEntry(final BibEntry entry) throws JabRefException {
        if( lt.getStatus() != WRITING ) throw new JabRefException("Indexer not in writing state");

        if( !entry.getCiteKeyOptional()
                .isPresent() ) {
            LOGGER.info("Not updating entry without cite key");
            return;
        }

        final String key = entry.getCiteKeyOptional()
                .get();

        final List<File> files = getListOfLinkedFiles(Collections.singletonList(entry), databaseContext.getFileDirectories(Globals.prefs.getFileDirectoryPreferences()));
        if( files.isEmpty() ) {
            LOGGER.warn("Not indexing entry " + key + " , which is without files");
            return;
        }

        for( final File file : files )
            lt.indexFile(new LuceneID(file, key));
    }


    private void removeEntry(final BibEntry entry) throws JabRefException {

        if( lt.getStatus() != WRITING ) throw new JabRefException("Indexer not in writing state");

        if( !entry.getCiteKeyOptional()
                .isPresent() ) {
            LOGGER.info("Not removing entry without cite key");
            return;
        }

        final String key = entry.getCiteKeyOptional()
                .get();

        LOGGER.info("Removing entry data for cite key " + key);
        try {
            lt.removeFile(new LuceneID(null, key));
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * Gives the set of all non-up-to-date entries in the database
     */
    private Set<LuceneID> isNotUpToDate() throws JabRefException {
        if( lt.getStatus() != SEARCHABLE && lt.getStatus() != WRITING )
            throw new JabRefException("Indexer not in searchable or writable state");

        return databaseContext.getDatabase()
                .getEntries()
                .stream()
                .map(this::isUpToDate)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    private Set<LuceneID> isUpToDate(BibEntry bibEntry) {


    }


    public Set<String> searchForString(String query) throws JabRefException {
        return lt.searchForString(query);
    }
}
