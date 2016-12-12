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

import com.google.common.eventbus.Subscribe;
import net.sf.jabref.Globals;
import net.sf.jabref.JabRefException;
import net.sf.jabref.fulltext.extractor.PDFTextExtractor;
import net.sf.jabref.model.database.BibDatabaseContext;
import net.sf.jabref.model.database.event.EntryAddedEvent;
import net.sf.jabref.model.database.event.EntryRemovedEvent;
import net.sf.jabref.model.entry.BibEntry;
import net.sf.jabref.model.entry.event.FieldChangedEvent;
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
import static net.sf.jabref.logic.util.io.FileUtil.getListOfLinkedFiles;

public class FullTextIndexer {

    private static final int MAX_DOCS = 9999;
    private final StandardAnalyzer analyzer = new StandardAnalyzer();
    private final IndexWriterConfig config= new IndexWriterConfig(analyzer);
    private IndexSearcher searcher;
    private IndexReader reader;

    enum LUCENE_FIELDS { FULL_CONTENT, KEY, FILE_NAME }

    private static final Log LOGGER = LogFactory.getLog(FullTextIndexer.class);

    private final BibDatabaseContext databaseContext;
    private IndexWriter iw;
    private Directory index;
    private boolean ready = false;

    public boolean isReady() {
        return ready;
    }

    public FullTextIndexer(final BibDatabaseContext databaseContext)  {
        this.databaseContext = databaseContext;
    }

    public boolean open() throws JabRefException {
        final Optional<Path> dbPath = databaseContext.getDatabasePath();

        if( !dbPath.isPresent() )
            throw new JabRefException("cannot run indexer on unsaved database");

        try {
            index = FSDirectory.open(Paths.get(dbPath.get()+".lucene"));
        } catch (final IOException e) {
            ready = false;
            throw new JabRefException("Exception opening/creating the fulltext index");
        }

        databaseContext.getDatabase().registerListener(this);

        ready = true;
        return ready;
    }


    private void prepareForWriting() throws JabRefException {
        ready = false;
        try {
            if( reader != null ) {
                searcher = null;
                reader.close();
            }
            iw = new IndexWriter(index, config);
        } catch (final IOException e) {
            throw new JabRefException("Exception creating the index writer");
        }
    }

    private void doneWriting() throws JabRefException {
        try {
            iw.close();

            reader = DirectoryReader.open(index);
            searcher = new IndexSearcher(reader);
        } catch (final IOException e) {
            throw new JabRefException("Exception closing the index writer");
        }
        ready = true;
    }

    public void recreateIndex() throws JabRefException {
        prepareForWriting();

        try {
            ready = false;
            iw.deleteAll();
            for( final BibEntry entry: databaseContext.getDatabase().getEntries() ) {
                updateEntry(entry);
            }

            doneWriting();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private void updateEntry(final BibEntry entry) throws JabRefException {
        if( !entry.getCiteKeyOptional().isPresent() )
            return;

        final String id = entry.getId();
        final String key = entry.getCiteKeyOptional().get();

        final List<File> files = getListOfLinkedFiles(Collections.singletonList(entry), databaseContext.getFileDirectories(Globals.prefs.getFileDirectoryPreferences()));
        if( files.isEmpty() ) {
            LOGGER.warn("Not indexing entry without a citekey");
            return;
        }

        final Document doc = new Document();
        for( final File file: files ) {

            final String fileName = file.toString();
            LOGGER.debug(id +" / "+key+" / "+fileName);

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

    public Set<String> searchForString(final String queryStr) throws JabRefException { // RegexpQuery, FieldValueQuery
        LOGGER.debug("Searching full-text (simple) for "+queryStr);
        final Query q;
        try {
            q = new QueryParser(FULL_CONTENT.name(), analyzer).parse(queryStr);

            final TopDocs docs = searcher.search(q, MAX_DOCS);

            return Arrays.stream(docs.scoreDocs)
                    .map(sd -> sd.doc)
                    .map(i -> getDocument(searcher, i))
                    .filter(Objects::nonNull)
                    .map(FullTextIndexer::getKey)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

        } catch (final ParseException e) {
            throw new JabRefException("Lucene query is invalid");
        } catch (final IOException e) {
            throw new JabRefException("Error in Lucene search");
        }
    }

    private static Document getDocument(final IndexSearcher searcher, final int i) {
        try {
            return searcher.doc(i, Collections.singleton(KEY.name()));
        } catch (final IOException e) {
            return null;
        }
    }

    private static String getKey(final Document d) {
        final String[] keys = d.getValues(KEY.name());
        if( keys.length != 1 ) {
            LOGGER.warn("got "+keys.length+" results");
            return null;
        }
        return keys[0];
    }

    @Subscribe
    public void listen(FieldChangedEvent fieldChangedEvent) {
        if (fieldChangedEvent.getFieldName().equals("file")) {
            LOGGER.info("file change: recompute");
        }
        if (fieldChangedEvent.getFieldName().equals(BibEntry.KEY_FIELD)) {
            LOGGER.info("update the entry in lucene, changing its ID");
        }
    }

    @Subscribe
    public void listen(EntryRemovedEvent entryRemovedEvent) {
        Optional<String> citeKey = entryRemovedEvent.getBibEntry().getCiteKeyOptional();
        if (citeKey.isPresent()) {
            LOGGER.info("removing entries for removed entry");
        }
    }

    @Subscribe
    public void listen(EntryAddedEvent entryAddedEvent) {
        Optional<String> citekey = entryAddedEvent.getBibEntry().getCiteKeyOptional();
        if (citekey.isPresent()) {
            LOGGER.info("indexing new entries for new entry");
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
