package net.sf.jabref.fulltext.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import javax.swing.JOptionPane;

import com.google.common.eventbus.Subscribe;
import net.sf.jabref.Globals;
import net.sf.jabref.JabRefException;
import net.sf.jabref.JabRefExecutorService;
import net.sf.jabref.fulltext.extractor.PDFTextExtractor;
import net.sf.jabref.gui.worker.Worker;
import net.sf.jabref.model.database.BibDatabaseContext;
import net.sf.jabref.model.database.event.EntryAddedEvent;
import net.sf.jabref.model.database.event.EntryRemovedEvent;
import net.sf.jabref.model.entry.BibEntry;
import net.sf.jabref.model.entry.event.FieldChangedEvent;
import net.sf.jabref.model.metadata.event.MetaDataChangedEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import static net.sf.jabref.fulltext.indexing.FullTextIndexer.LUCENE_FIELDS.FILE_MODTIME;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.LUCENE_FIELDS.FILE_NAME;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.LUCENE_FIELDS.FULL_CONTENT;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.LUCENE_FIELDS.KEY;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.Status.CLOSED;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.Status.NOT_IN_USE;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.Status.SEARCHABLE;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.Status.WRITING;
import static net.sf.jabref.logic.util.io.FileUtil.getListOfLinkedFiles;

public class FullTextIndexer implements Worker {

    private static final int MAX_DOCS = 9999;
    private static final long PERIOD = 5000L;

    private final StandardAnalyzer analyzer = new StandardAnalyzer();
    private IndexSearcher searcher;
    private IndexReader reader;
    private Set<LuceneID> toUpdate;
    private ScheduledFuture<?> futureTask;

    @Override
    public void run() {
        if( toUpdate.isEmpty() )
            return;

        try {
            openWriter();
            int res = JOptionPane.showConfirmDialog(null, "There are " + toUpdate.size() + " files to reindex. Do you want to do it now?", "Reindex", JOptionPane.YES_NO_OPTION);
            if( res == JOptionPane.YES_OPTION ) {
                LOGGER.info("Re-indexing.");
                toUpdate.forEach(d -> {
                    updateLuceneDocument(d);
                    toUpdate.remove(d);
                });
            }
            closeWriter();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    enum LUCENE_FIELDS {FULL_CONTENT, KEY, FILE_NAME, FILE_MODTIME}

    enum Status {NOT_IN_USE, CLOSED, WRITING, SEARCHABLE}

    private Status status = NOT_IN_USE;

    private static final Log LOGGER = LogFactory.getLog(FullTextIndexer.class);

    private static class LuceneID {
        File file;
        String citeKey;
        Integer id;

        LuceneID(File file, String citeKey, Integer id) {
            this.file = file;
            this.citeKey = citeKey;
            this.id = id;
        }
        LuceneID(LuceneID pre, int d) {
            this(pre.file,pre.citeKey, d);
        }
        LuceneID(File file, String citeKey) {
            this(file, citeKey, null);
        }

    }

    private final BibDatabaseContext databaseContext;
    private IndexWriter iw;
    private Directory directory;

    public FullTextIndexer(final BibDatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    public void setup() throws JabRefException {
        LOGGER.debug("Setting up indexer");
        final Optional<Path> dbPath = databaseContext.getDatabasePath();

        if( !dbPath.isPresent() ) throw new JabRefException("cannot run indexer on unsaved database");

        Path indexDir = Paths.get(dbPath.get() + ".lucene");
        try {
            directory = FSDirectory.open(indexDir);
            status = CLOSED;
            if( directory.listAll().length == 0 ) { // new
                LOGGER.warn("Index is still empty");
                new IndexWriter(directory, new IndexWriterConfig(analyzer)).close();
            }
            open();
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

    public void open() throws JabRefException {
        LOGGER.debug("Opening reader");
        switch( status ) {
            case SEARCHABLE:
            case WRITING:
                LOGGER.warn("Indexer is already open, status " + status);
                break;
            case NOT_IN_USE:
                throw new JabRefException("Indexer is not in use: should not be listening");
            case CLOSED:
                try {
                    reader = DirectoryReader.open(directory);
                    searcher = new IndexSearcher(reader);
                    status = SEARCHABLE;
                } catch (final IOException e) {
                    //e.printStackTrace();
                    throw new JabRefException("Error closing the indexer reader", e);
                }
                break;
        }
    }

    public void close() throws JabRefException {
        LOGGER.debug("Closing index");
        switch( status ) {
            case NOT_IN_USE:
                throw new JabRefException("Indexer is not in use: should not be listening");
            case CLOSED:
                throw new JabRefException("Indexer should be open, but it is not");
            case WRITING:
                closeWriter();
            case SEARCHABLE:
                try {
                    reader.close();
                    searcher = null;
                    status = CLOSED;
                } catch (final IOException e) {
                    throw new JabRefException("Error closing the indexer reader");
                }
        }
    }


    private void openWriter() throws JabRefException {
        LOGGER.debug("Opening writer");
        switch( status ) {
            case WRITING:
                LOGGER.warn("Indexer is already in writing state: should not happen");
                break;
            case NOT_IN_USE:
                throw new JabRefException("Indexer is not in use: should not be listening");
            case CLOSED:
                open();
            case SEARCHABLE:
                try {
                    iw = new IndexWriter(directory, new IndexWriterConfig(analyzer));
                    status = WRITING;
                } catch (final IOException e) {
                    throw new JabRefException("Exception creating the index writer");
                }
        }

    }

    private void closeWriter() throws JabRefException {
        LOGGER.debug("Closing writer");
        switch( status ) {
            case NOT_IN_USE:
                throw new JabRefException("Indexer is not in use: should not be listening");
            case SEARCHABLE:
                throw new JabRefException("Indexer is in reading state: should not happen");
            case CLOSED:
                throw new JabRefException("Indexer is already closed: should not happen");
            case WRITING:
                try {
                    iw.close();
                    status = SEARCHABLE;

                    // reload the index
                    close();
                    open();
                } catch (final IOException e) {
                    throw new JabRefException("Exception closing the index writer");
                }
                break;
        }
    }



    public void destroyDB() throws JabRefException {
        if( status != CLOSED )
            throw new JabRefException("Close indexer before destroying it");

        LOGGER.debug("Destroying DB");
        databaseContext.getDatabase()
                .unregisterListener(this);

        futureTask.cancel(true);

        status = NOT_IN_USE;
    }


    public void recreateIndex() throws JabRefException {
        openWriter();

        try {
            LOGGER.debug("recreating. Deleting everything");
            iw.deleteAll();
            for( final BibEntry entry: databaseContext.getDatabase().getEntries() ) {
                insertEntry(entry);
            }

            closeWriter();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private void indexFile(LuceneID lID) {
        final File file = lID.file;
        final String fileName = file.getName();
        final String key = lID.citeKey;
        LOGGER.debug("Indexing "+fileName+" for "+key);

        if( !fileName.endsWith("pdf") ) {
            LOGGER.warn("Only PDF files can be indexed at this time");
            return;
        }

        final Document doc = new Document();
        doc.add(new StringField(KEY.name(), key, Store.YES));
        doc.add(new StringField(FILE_NAME.name(), fileName, Store.YES));
        doc.add(new StoredField(FILE_MODTIME.name(), file.lastModified()));

        try {
            final String contents = PDFTextExtractor.extractPDFText(file);
            doc.add(new TextField(FULL_CONTENT.name(), contents, Store.NO));

            iw.addDocument(doc);
        } catch (final IOException|JabRefException  e) {
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    private void insertEntry(final BibEntry entry) throws JabRefException {
        if( status != WRITING )
            throw new JabRefException("Indexer not in writing state");

        if( !entry.getCiteKeyOptional().isPresent() ) {
            LOGGER.info("Not updating entry without cite key");
            return;
        }

        final String key = entry.getCiteKeyOptional().get();

        final List<File> files = getListOfLinkedFiles(Collections.singletonList(entry), databaseContext.getFileDirectories(Globals.prefs.getFileDirectoryPreferences()));
        if( files.isEmpty() ) {
            LOGGER.warn("Not indexing entry "+key+" , which is without files");
            return;
        }

        for( final File file: files )
            indexFile(new LuceneID(file, key));
    }

    private void removeFile(LuceneID lID) throws IOException {
        final File file = lID.file;
        //final String fileName = file.getName();
        final String key = lID.citeKey;
        LOGGER.debug("Removing file "+file+" for key "+key);
        if( file != null )
            iw.deleteDocuments(new Term(KEY.name(), key), new Term(FILE_NAME.name(), file.toString()));
        else
            iw.deleteDocuments(new Term(KEY.name(), key));
    }

    private void removeEntry(final BibEntry entry) throws JabRefException {

        if( status != WRITING )
            throw new JabRefException("Indexer not in writing state");

        if( !entry.getCiteKeyOptional().isPresent() ) {
            LOGGER.info("Not removing entry without cite key");
            return;
        }

        final String key = entry.getCiteKeyOptional().get();

        LOGGER.info("Removing entry data for cite key "+key);
        try {
            removeFile(new LuceneID(null, key));
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private void changeEntryKey(final String oldKey, final String newKey) throws JabRefException {
        try {
            LOGGER.debug("change entry from "+oldKey+" to "+newKey);
            iw.updateDocValues(new Term(KEY.name(), oldKey), new StringField(KEY.name(), newKey, Store.YES));
        } catch (final IOException e) {
            throw new JabRefException("Error updating", e);
        }
    }
    /**
     * Returns the citation keys of all matching BibEntries
     *
     * @param queryStr The query, in standard Lucene syntax
     * @return A set containing the citation keys of the matching BibEntry s
     */
    public Set<String> searchForString(final String queryStr) throws JabRefException { // RegexpQuery, FieldValueQuery
        if( status != SEARCHABLE && status != WRITING )
            throw new JabRefException("Indexer is not in status searchable");

        final Query q;
        try {
            q = new QueryParser(FULL_CONTENT.name(), analyzer).parse(queryStr);

            final TopDocs docs = searcher.search(q, MAX_DOCS);

            return Arrays.stream(docs.scoreDocs)
                    .map(sd -> sd.doc)
                    .map(this::getLuceneDocument)
                    .filter(Objects::nonNull)
                    .map(FullTextIndexer::getLuceneKey)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

        } catch (final ParseException e) {
            throw new JabRefException("Lucene query is invalid");
        } catch (final IOException e) {
            throw new JabRefException("Error in Lucene search");
        }
    }


    private Document getLuceneDocument(final int i) {
        try {
            return searcher.doc(i, Collections.singleton(KEY.name()));
        } catch (final IOException e) {
            return null;
        }
    }

    private static String getLuceneKey(final Document d) {
        final String[] keys = d.getValues(KEY.name());
        if( keys.length != 1 ) {
            LOGGER.warn("got "+keys.length+" results");
            return null;
        }
        return keys[0];
    }

    @Subscribe
    public void listen(final FieldChangedEvent fieldChangedEvent) {
        final String oldValue = fieldChangedEvent.getOldValue();
        final String newValue = fieldChangedEvent.getNewValue();
        if ( "file".equals(fieldChangedEvent.getFieldName()) ) {
            LOGGER.debug("file change: recompute");
            LOGGER.debug(oldValue +" to "+ newValue);

            fieldChangedEvent.getBibEntry().getCiteKeyOptional().ifPresent( key -> {

                final Set<File> oldFiles = new HashSet<>(getListOfLinkedFiles(oldValue, databaseContext.getFileDirectories(Globals.prefs.getFileDirectoryPreferences())));
                final Set<File> newFiles = new HashSet<>(getListOfLinkedFiles(newValue, databaseContext.getFileDirectories(Globals.prefs.getFileDirectoryPreferences())));

                final Set<File> added = new HashSet<>(newFiles);
                added.removeAll(oldFiles);

                final Set<File> removed = new HashSet<>(oldFiles);
                removed.removeAll(newFiles);

                try {
                    LOGGER.info("update in lucene, file " + oldValue + " to " + newValue);
                    switch( status ) {
                        case SEARCHABLE:
                        case CLOSED:
                            openWriter();
                            for( final File f: added) indexFile(new LuceneID(f, key));
                            for( final File f: removed) removeFile(new LuceneID(f, key));
                            closeWriter();
                            break;
                        case WRITING:
                            changeEntryKey(oldValue, newValue);
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
        if (fieldChangedEvent.getFieldName().equals(BibEntry.KEY_FIELD)) {
            try {
                LOGGER.info("update in lucene, changing ID from "+oldValue+" to "+newValue);
                switch( status ) {
                    case SEARCHABLE:
                    case CLOSED:
                        openWriter();
                        changeEntryKey(oldValue, newValue);
                        closeWriter();
                        break;
                    case WRITING:
                        changeEntryKey(oldValue, newValue);
                        break;
                    case NOT_IN_USE:
                        LOGGER.warn("Not in use: should not be listening");
                        break;
                }
            } catch(final Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Subscribe
    public void listen(final EntryRemovedEvent entryRemovedEvent) {
        final BibEntry entry = entryRemovedEvent.getBibEntry();
        try {
            switch( status ) {
                case SEARCHABLE:
                case CLOSED:
                    openWriter();
                    removeEntry(entry);
                    closeWriter();
                    break;
                case WRITING:
                    removeEntry(entry);
                    break;
                case NOT_IN_USE:
                    LOGGER.warn("Not in use: should not be listening");
                    break;
            }
        } catch(final Exception e) {
            e.printStackTrace();
        }
    }

    @Subscribe
    public void listen(final EntryAddedEvent entryAddedEvent) {
        final BibEntry entry = entryAddedEvent.getBibEntry();
        try {
            switch( status ) {
                case SEARCHABLE:
                case CLOSED:
                    openWriter();
                    insertEntry(entry);
                    closeWriter();
                    break;
                case WRITING:
                    insertEntry(entry);
                    break;
                case NOT_IN_USE:
                    LOGGER.warn("Not in use: should not be listening");
                    break;
            }
        } catch(final Exception e) {
            e.printStackTrace();
        }
    }


    @Subscribe
    public void listen(final MetaDataChangedEvent event) {
        LOGGER.debug(status);
        try {
            if( status != NOT_IN_USE && !event.getMetaData()
                    .isFullTextIndexed() ) {
                LOGGER.debug("Time to destroy the world");
                close();
                destroyDB();
            } else if( status == NOT_IN_USE && event.getMetaData()
                    .isFullTextIndexed() ) {
                LOGGER.debug("Going live");
                setup();
                open();
            }
        } catch(final JabRefException e) {
            LOGGER.error(e.getLocalizedMessage());
        }
    }

    private void updateLuceneDocument(LuceneID d) {
        if( d.id != null )
            deleteFromIndex(d);
        indexFile(d);
    }



    private void deleteFromIndex(LuceneID lID) {
        try {
            iw.tryDeleteDocument(reader, lID.id);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Gives the set of all non-up-to-date entries in the database
     */
    private Set<LuceneID> isNotUpToDate() throws JabRefException {
        if( status != SEARCHABLE && status != WRITING )
            throw new JabRefException("Indexer not in searchable or writable state");

        return databaseContext.getDatabase()
                .getEntries()
                .stream()
                .map(this::isNotUpToDate)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Gives the set of all non-up-to-date entries of an entry
     * @return
     */
    private Set<LuceneID> isNotUpToDate(final BibEntry entry)  {

        if( !entry.getCiteKeyOptional().isPresent() ) {
            LOGGER.info("Not checking entry without cite key");
            return Collections.emptySet();
        }

        final String key = entry.getCiteKeyOptional().get();

        final List<File> files = getListOfLinkedFiles(Collections.singletonList(entry), databaseContext.getFileDirectories(Globals.prefs.getFileDirectoryPreferences()));
        if( files.isEmpty() ) {
            LOGGER.warn("Not indexing entry "+key+" , which is without files");
            return Collections.emptySet();
        }

        final Set<LuceneID> res = new HashSet<>();
        files.forEach(file -> {
            LuceneID lID = new LuceneID(file, key);
            Optional<Integer> id = lookupDocument(lID);

            if( id.isPresent() ) {
                lID.id = id.get();
                if( isUpToDate(lID) )
                    return;
            }

            res.add(lID);
        });

        return res;
    }

    private boolean isUpToDate(LuceneID lID) {
        try {
            StoredField fLaft = (StoredField) reader.document(lID.id)
                    .getField(FILE_MODTIME.name());
            return lID.file.lastModified() <= fLaft.numericValue()
                    .longValue();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }


    /**
     * Looks for a document in the index, by file name and cite key
     * @param lID
     * @return the LuceneID, with doc ID, if it exists. Empty otherwise
     */
    private Optional<Integer> lookupDocument(LuceneID lID) {
        Query q = new Builder()
                .add(new TermQuery(new Term(KEY.name(), lID.citeKey)), Occur.MUST)
                .add(new TermQuery(new Term(FILE_NAME.name(), lID.file.getName())), Occur.MUST)
                .build();

        try {
            TopDocs doc = searcher.search(q, 2);
            if( doc.scoreDocs.length == 0 ) {
                LOGGER.warn("No document found for file "+lID.file+" and entry "+lID.citeKey);
                return Optional.empty();
            } else if( doc.scoreDocs.length > 1 ) {
                LOGGER.error("More than one document found for file "+lID.file+" and entry "+lID.citeKey);
                return Optional.empty();
            }
            return Optional.of(doc.scoreDocs[0].doc);

        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }



}
