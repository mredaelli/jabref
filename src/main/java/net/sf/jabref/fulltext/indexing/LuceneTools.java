package net.sf.jabref.fulltext.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import net.sf.jabref.JabRefException;
import net.sf.jabref.fulltext.extractor.PDFTextExtractor;
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

import static net.sf.jabref.fulltext.indexing.LuceneTools.LUCENE_FIELDS.FILE_MODTIME;
import static net.sf.jabref.fulltext.indexing.LuceneTools.LUCENE_FIELDS.FILE_NAME;
import static net.sf.jabref.fulltext.indexing.LuceneTools.LUCENE_FIELDS.FULL_CONTENT;
import static net.sf.jabref.fulltext.indexing.LuceneTools.LUCENE_FIELDS.KEY;
import static net.sf.jabref.fulltext.indexing.LuceneTools.Status.NOT_IN_USE;
import static net.sf.jabref.fulltext.indexing.LuceneTools.Status.WRITING;

class LuceneTools {

    private static final Log LOGGER = LogFactory.getLog(LuceneTools.class);
    private static final int MAX_DOCS = 9999;
    private final StandardAnalyzer analyzer = new StandardAnalyzer();
    private Status status = NOT_IN_USE;
    private IndexSearcher searcher;
    private IndexWriter iw;
    private IndexReader reader;
    private Directory directory;

    private static String getLuceneKey(final Document d) {
        final String[] keys = d.getValues(KEY.name());
        if( keys.length != 1 ) {
            LOGGER.warn("got " + keys.length + " results");
            return null;
        }
        return keys[0];
    }

    public boolean isEmpty() throws IOException {
        return directory.listAll().length == 0;
    }

    public void empty() throws IOException {
        iw.deleteAll();
    }

    void setUnused() {
        status = NOT_IN_USE;
    }

    public Directory getDirectory() {
        return directory;
    }

    public void setDirectory(Path directory) throws IOException {
        this.directory = FSDirectory.open(directory);
    }

    Status getStatus() {
        return status;
    }

    void createEmptyIndex() throws IOException {
        new IndexWriter(directory, new IndexWriterConfig(analyzer)).close();
    }

    public void open() throws JabRefException {
        LOGGER.debug("Opening reader");
        switch( status ) {
            case SEARCHABLE:
            case WRITING:
                LOGGER.warn("Indexer is already open, status " + status);
                break;
            case NOT_IN_USE:
                //throw new JabRefException("Indexer is not in use: should not be listening");
            case CLOSED:
                try {
                    reader = DirectoryReader.open(directory);
                    searcher = new IndexSearcher(reader);
                    status = Status.SEARCHABLE;
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
                    status = Status.CLOSED;
                } catch (final IOException e) {
                    throw new JabRefException("Error closing the indexer reader");
                }
        }
    }

    void openWriter() throws JabRefException {
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

    void closeWriter() throws JabRefException {
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
                    status = Status.SEARCHABLE;

                    // reload the index
                    close();
                    open();
                } catch (final IOException e) {
                    throw new JabRefException("Exception closing the index writer");
                }
                break;
        }
    }

    void indexFile(LuceneID lID) {
        final File file = lID.file;
        final String fileName = file.getName();
        final String key = lID.citeKey;
        LOGGER.debug("Indexing " + fileName + " for " + key);

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
        } catch (final IOException | JabRefException e) {
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }
    }

    void removeFile(LuceneID lID) throws IOException {
        final File file = lID.file;
        //final String fileName = file.getName();
        final String key = lID.citeKey;
        LOGGER.debug("Removing file " + file + " for key " + key);
        if( file != null ) iw.deleteDocuments(new Term(KEY.name(), key), new Term(FILE_NAME.name(), file.toString()));
        else iw.deleteDocuments(new Term(KEY.name(), key));
    }

    void changeEntryKey(final String oldKey, final String newKey) throws JabRefException {
        try {
            LOGGER.debug("change entry from " + oldKey + " to " + newKey);
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
        if( status != Status.SEARCHABLE && status != WRITING )
            throw new JabRefException("Indexer is not in status searchable");

        final Query q;
        try {
            q = new QueryParser(FULL_CONTENT.name(), analyzer).parse(queryStr);

            final TopDocs docs = searcher.search(q, MAX_DOCS);

            return Arrays.stream(docs.scoreDocs)
                    .map(sd -> sd.doc)
                    .map(this::getLuceneDocument)
                    .filter(Objects::nonNull)
                    .map(LuceneTools::getLuceneKey)
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

    void updateLuceneDocument(LuceneID d) {
        if( d.id != null ) deleteFromIndex(d);
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
     * Looks for a document in the index, by file name and cite key
     *
     * @param lID
     * @return the LuceneID, with doc ID, if it exists. Empty otherwise
     */
    Optional<Integer> lookupDocument(LuceneID lID) {
        Query q = new Builder().add(new TermQuery(new Term(KEY.name(), lID.citeKey)), Occur.MUST)
                .add(new TermQuery(new Term(FILE_NAME.name(), lID.file.getName())), Occur.MUST)
                .build();

        try {
            TopDocs doc = searcher.search(q, 2);
            if( doc.scoreDocs.length == 0 ) {
                LOGGER.warn("No document found for file " + lID.file + " and entry " + lID.citeKey);
                return Optional.empty();
            } else if( doc.scoreDocs.length > 1 ) {
                LOGGER.error("More than one document found for file " + lID.file + " and entry " + lID.citeKey);
                return Optional.empty();
            }
            return Optional.of(doc.scoreDocs[0].doc);

        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    boolean isUpToDate(LuceneID lID) {
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

    enum Status {NOT_IN_USE, CLOSED, WRITING, SEARCHABLE}

    enum LUCENE_FIELDS {FULL_CONTENT, KEY, FILE_NAME, FILE_MODTIME}


}
