package net.sf.jabref.fulltext.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import net.sf.jabref.JabRefException;
import net.sf.jabref.fulltext.extractor.PDFTextExtractor;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class Indexer {

    void b(final Path directoryFile) throws JabRefException {
        final StandardAnalyzer analyzer = new StandardAnalyzer();

        final Directory index;
        try {
            index = FSDirectory.open(directoryFile);
        } catch (final IOException e) {
            throw new JabRefException("Exception opening/creating the fulltext index");
        }

        final IndexWriterConfig config = new IndexWriterConfig(analyzer);

        final IndexWriter w;
        try {
            w = new IndexWriter(index, config);
        } catch (final IOException e) {
            throw new JabRefException("Exception creating the index writer");
        }


            addDoc(w, new File(""));
            //addDoc(w, "Lucene for Dummies", "55320055Z");
            //addDoc(w, "Managing Gigabytes", "55063554A");
            //addDoc(w, "The Art of Computer Science", "9900333X");

        try {
            w.close();
        } catch (IOException e) {
            throw new JabRefException("Error closing");
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
    }

    private static void addDoc(final IndexWriter w, File pdfFile) throws JabRefException {
        final Document doc = new Document();

        doc.add(new TextField("fullcontent", PDFTextExtractor.extractPDFText(pdfFile), Field.Store.YES));

        /*doc.add(new StringField("isbn", isbn, Field.Store.YES));*/
        try {
            w.addDocument(doc);
        } catch (IOException e) {
            throw new JabRefException("Error adding document");
        }
    }
}
