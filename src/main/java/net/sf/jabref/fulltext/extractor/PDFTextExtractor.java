package net.sf.jabref.fulltext.extractor;

import java.io.File;
import java.io.IOException;

import net.sf.jabref.JabRefException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

public class PDFTextExtractor {

    private static final Log LOGGER = LogFactory.getLog(PDFTextExtractor.class);

    public static String extractPDFText(final File file) throws JabRefException {
        try {
            final PDDocument doc = PDDocument.load(file);
            final PDFTextStripper stripper = new PDFTextStripper();

            stripper.setLineSeparator("\n");
            stripper.setStartPage(1);
            //stripper.setEndPage(5);// this mean that it will index the first 5 pages only
            return stripper.getText(doc);

        } catch (final IOException e) {
            LOGGER.error(e.getMessage());
            //e.printStackTrace();
            throw new JabRefException("Error getting text from PDF file "+file);
        }
    }
}
