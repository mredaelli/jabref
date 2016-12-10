package net.sf.jabref.fulltext.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import net.sf.jabref.Globals;
import net.sf.jabref.JabRefException;
import net.sf.jabref.fulltext.extractor.PDFTextExtractor;
import net.sf.jabref.model.database.BibDatabaseContext;
import net.sf.jabref.model.entry.BibEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import static net.sf.jabref.logic.util.io.FileUtil.getListOfLinkedFiles;

public class FullTextIndexer {

    private static final Log LOGGER = LogFactory.getLog(FullTextIndexer.class);

    private final BibDatabaseContext databaseContext;
    private IndexWriter iw;
    private Directory index;

    public FullTextIndexer(final BibDatabaseContext databaseContext)  {
        this.databaseContext = databaseContext;
    }

    public void create() throws JabRefException {
        final Optional<Path> dbPath = databaseContext.getDatabasePath();

        if( !dbPath.isPresent() )
            throw new JabRefException("cannot run indexer on unsaved database");

        final StandardAnalyzer analyzer = new StandardAnalyzer();

        try {
            index = FSDirectory.open(Paths.get(dbPath.get()+".lucene"));
        } catch (final IOException e) {
            throw new JabRefException("Exception opening/creating the fulltext index");
        }

        final IndexWriterConfig config = new IndexWriterConfig(analyzer);

        try {
            iw = new IndexWriter(index, config);
        } catch (final IOException e) {
            throw new JabRefException("Exception creating the index writer");
        }
    }


    public void recreateIndex() throws JabRefException {
        try {
            iw.deleteAll();
            for( final BibEntry entry: databaseContext.getDatabase().getEntries() ) {
                updateEntry(entry);
            }
            iw.close();
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
        if( files.isEmpty() )
            return;

        final Document doc = new Document();
        for( final File file: files ) {

            final String fileName = file.toString();
            LOGGER.info(id +" / "+key+" / "+fileName);

            doc.add(new StringField("key", key, Field.Store.YES));
            doc.add(new StringField("filename", fileName, Field.Store.YES));
            doc.add(new TextField("fullcontent", PDFTextExtractor.extractPDFText(file), Field.Store.YES));

            try {
                iw.addDocument(doc);
            } catch (final IOException e) {
                throw new JabRefException("Error adding document");
            }
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
