package net.sf.jabref.fulltext.indexing;

import java.io.File;

class LuceneID {
    File file;
    String citeKey;
    Integer id;

    LuceneID(File file, String citeKey, Integer id) {
        this.file = file;
        this.citeKey = citeKey;
        this.id = id;
    }

    LuceneID(LuceneID pre, int d) {
        this(pre.file, pre.citeKey, d);
    }

    LuceneID(File file, String citeKey) {
        this(file, citeKey, null);
    }

}
