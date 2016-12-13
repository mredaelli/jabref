package net.sf.jabref.fulltext.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import net.sf.jabref.Globals;
import net.sf.jabref.JabRefException;
import net.sf.jabref.fulltext.extractor.PDFTextExtractor;
import net.sf.jabref.logic.util.io.FileUtil;
import net.sf.jabref.model.database.BibDatabaseContext;
import net.sf.jabref.model.database.event.EntryAddedEvent;
import net.sf.jabref.model.database.event.EntryRemovedEvent;
import net.sf.jabref.model.entry.BibEntry;
import net.sf.jabref.model.entry.event.FieldChangedEvent;
import net.sf.jabref.model.metadata.event.MetaDataChangedEvent;

import com.google.common.eventbus.Subscribe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import static net.sf.jabref.fulltext.indexing.FullTextIndexer.LUCENE_FIELDS.FILE_NAME;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.LUCENE_FIELDS.FULL_CONTENT;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.LUCENE_FIELDS.KEY;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.Status.CLOSED;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.Status.NOT_IN_USE;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.Status.SEARCHABLE;
import static net.sf.jabref.fulltext.indexing.FullTextIndexer.Status.WRITING;
import static net.sf.jabref.logic.util.io.FileUtil.getListOfLinkedFiles;

/* TODO: when duplicating an entry, the bibtex key is the same, so the same documents are added twice, and when removed, everything is removed
    - use the unique identifier temporarily?
    -  automatically make the CiteKey unique?
 */
public class FullTextIndexer {

    private static final int MAX_DOCS = 9999;
    private final StandardAnalyzer analyzer = new StandardAnalyzer();
    private IndexSearcher searcher;
    private IndexReader reader;
    private Path indexDir;

    enum LUCENE_FIELDS {FULL_CONTENT, KEY, FILE_NAME}

    enum Status {NOT_IN_USE, CLOSED, WRITING, SEARCHABLE}

    private Status status = NOT_IN_USE;

    private static final Log LOGGER = LogFactory.getLog(FullTextIndexer.class);

    private final BibDatabaseContext databaseContext;
    private IndexWriter iw;
    private Directory directory;

    public FullTextIndexer(final BibDatabaseContext databaseContext) {
        this.databaseContext = databaseContext;
    }

    public void setup() throws JabRefException {
        final Optional<Path> dbPath = databaseContext.getDatabasePath();

        if( !dbPath.isPresent() ) throw new JabRefException("cannot run indexer on unsaved database");

        indexDir = Paths.get(dbPath.get() + ".lucene");
        try {
            directory = FSDirectory.open(indexDir);
            status = CLOSED;
            if( directory.listAll().length == 0 ) { // new
                LOGGER.warn("Index is still empty");
                new IndexWriter(directory, new IndexWriterConfig(analyzer)).close();
                recreateIndex();
            }
        } catch (final IOException e) {
            throw new JabRefException("Exception opening/creating the fulltext index", e);
        }

        databaseContext.getDatabase()
                .registerListener(this);
        databaseContext.getMetaData()
                .registerListener(this);
    }

    public void open() throws JabRefException {
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

    private void close() throws JabRefException {
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



    private void destroyDB() throws JabRefException {
        if( status != CLOSED )
            throw new JabRefException("Close indexer before destroying it");
        try {
            databaseContext.getDatabase()
                    .unregisterListener(this);
            databaseContext.getMetaData()
                    .unregisterListener(this);
            FileUtil.deleteTree(indexDir);
            status = NOT_IN_USE;
        } catch (final IOException e) {
            throw new JabRefException("Error deleting indexer files", e);
        }
    }


    public void recreateIndex() throws JabRefException {
        openWriter();

        try {
            iw.deleteAll();
            for( final BibEntry entry: databaseContext.getDatabase().getEntries() ) {
                updateEntry(entry);
            }

            closeWriter();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private void updateEntry(final BibEntry entry) throws JabRefException {
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

        final Document doc = new Document();
        for( final File file: files ) {

            final String fileName = file.toString();
            LOGGER.debug(key+" / "+fileName);

            if( !fileName.endsWith("pdf") ) {
                LOGGER.warn("Only PDF files can be indexed at this time");
                return;
            }

            doc.add(new StringField(KEY.name(), key, Field.Store.YES));
            doc.add(new StringField(FILE_NAME.name(), fileName, Field.Store.YES));
            final String contents = PDFTextExtractor.extractPDFText(file);
            doc.add(new TextField(FULL_CONTENT.name(), contents, Field.Store.NO));

            try {
                iw.addDocument(doc);
            } catch (final IOException e) {
                throw new JabRefException("Error adding document");
            }
        }
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
            iw.deleteDocuments(new Term(KEY.name(), key));
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the citation keys of all matching BibEntries
     *
     * @param queryStr The query, in standard Lucene syntax
     * @return A set containing the citation keys of the matching BibEntry s
     */
    public Set<String> searchForString(final String queryStr) throws JabRefException { // RegexpQuery, FieldValueQuery
        if( status != SEARCHABLE )
            throw new JabRefException("Indexer is not in status searchable");

        final Query q;
        try {
            q = new QueryParser(FULL_CONTENT.name(), analyzer).parse(queryStr);

            final TopDocs docs = searcher.search(q, MAX_DOCS);

            return Arrays.stream(docs.scoreDocs)
                    .map(sd -> sd.doc)
                    .map(i -> getLuceneDocument(searcher, i))
                    .filter(Objects::nonNull)
                    .map(d -> getLuceneKey(d))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

        } catch (final ParseException e) {
            throw new JabRefException("Lucene query is invalid");
        } catch (final IOException e) {
            throw new JabRefException("Error in Lucene search");
        }
    }


    private static Document getLuceneDocument(final IndexSearcher searcher, final int i) {
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
        if ( "file".equals(fieldChangedEvent.getFieldName()) ) {
            LOGGER.info("file change: recompute");
            LOGGER.info(fieldChangedEvent.getOldValue()+" to "+fieldChangedEvent.getOldValue());
        }
        if (fieldChangedEvent.getFieldName().equals(BibEntry.KEY_FIELD)) {
            LOGGER.info("update the entry in lucene, changing its ID");
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
                    updateEntry(entry);
                    closeWriter();
                    break;
                case WRITING:
                    updateEntry(entry);
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
        try {
            if( status != NOT_IN_USE && !event.getMetaData()
                    .isFullTextIndexed() ) {
                LOGGER.info("Time to destroy the world");
                close();
                destroyDB();
            } else if( status == NOT_IN_USE && event.getMetaData()
                    .isFullTextIndexed() ) {
                LOGGER.info("Going live");
                setup();
                open();
            }
        } catch(final JabRefException e) {
            LOGGER.error(e.getLocalizedMessage());
        }
    }




}


/*
        // 2. query
        final String querystr = args.length > 0 ? args[0] : "lucene";

        // the "title" arg specifies the default field to use
        // when no field is explicitly specified in the query.
        final Query q = new QueryParser("title", analyzer).parse(querystr);

        // 3. search
        final int hitsPerPage = 10;
        final IndexReader reader = DirectoryReader.open(index);
        final IndexSearcher searcher = new IndexSearcher(reader);
        final TopDocs docs = searcher.search(q, hitsPerPage);
        final ScoreDoc[] hits = docs.scoreDocs;

        // 4. display results
        System.out.println("Found " + hits.length + " hits.");
        for(int i=0;i<hits.length;++i) {
            final int docId = hits[i].doc;
            final Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". " + d.get("isbn") + '\t' + d.get("title"));
        }

        // reader can only be closed when there
        // is no need to access the documents any more.
        reader.close();
        */
