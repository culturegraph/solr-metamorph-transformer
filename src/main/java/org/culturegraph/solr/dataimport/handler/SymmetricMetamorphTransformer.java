package org.culturegraph.solr.dataimport.handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.handler.dataimport.Transformer;
import org.metafacture.biblio.marc21.Marc21Decoder;
import org.metafacture.fix.biblio.marc21.Marc21Encoder;
import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.helpers.DefaultObjectPipe;
import org.metafacture.framework.helpers.DefaultStreamPipe;
import org.metafacture.metamorph.InlineMorph;
import org.metafacture.metamorph.Metamorph;

public class SymmetricMetamorphTransformer extends Transformer {

    public static String MORPH_DEF = "morphDef";
    public static String FORMAT = "format";

    private Metamorph metamorph;
    private boolean isLoaded = false;
    private MetamorphFilter transformer;

    private DefaultObjectPipe<String, StreamReceiver> decoder;
    private DefaultStreamPipe<ObjectReceiver<String>> encoder;

    private Metamorph loadMetamorph(InputStream inputStream) {
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        Iterator<String> iter = br.lines().filter(s -> !s.startsWith("<?")).iterator();
        InlineMorph inline = InlineMorph.in(this);
        while (iter.hasNext()) {
            inline = inline.with(iter.next());
        }
        return inline.create();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object transformRow(Map<String, Object> row, Context context) {

        if (row == null || row.isEmpty()) {
            return row;
        }

        List<Map<String, String>> fields = context.getAllEntityFields();

        for (Map<String, String> field : fields) {
            String morphDef = context.replaceTokens(field.get(MORPH_DEF));

            if (morphDef == null || morphDef.isEmpty()) {
                continue;
            }

            if (!isLoaded) {
                final SolrCore core = context.getSolrCore();
                if (core != null) {
                    ResourceLoader loader = core.getResourceLoader();
                    try {
                        InputStream morphDefInputStream = loader.openResource(morphDef);
                        metamorph = loadMetamorph(morphDefInputStream);
                        isLoaded = true;
                    } catch (IOException e) {
                        String target = ((SolrResourceLoader) loader).resourceLocation(morphDef);
                        throw new IllegalArgumentException("MorphDef '" + target + "' not found.", e);
                    }
                } else {
                    // Log debugging
                    metamorph = new Metamorph(morphDef);
                    isLoaded = true;
                }

                String inputFormat = context.replaceTokens(field.get(FORMAT));
                if (inputFormat == null || inputFormat.isEmpty()) {
                    throw new IllegalArgumentException("Input format required!");
                }

                switch (inputFormat) {
                    case "marc21":
                        decoder = new Marc21Decoder();
                        encoder = new Marc21Encoder();
                        transformer = new MetamorphFilter(decoder, metamorph, encoder);
                        break;
                    default:
                        break;
                }
            }

            String columnName = field.get(DataImporter.COLUMN);

            Object value = row.get(columnName);
            if (value == null)
                continue;

            if (value instanceof String) {
                transformer.transform((String)value).ifPresent(s -> row.put(columnName, s));
            } else {
                List transformedRecordList = new ArrayList();
                List<?> values = (List<?>) value;
                for (String val: (List<String>) values) {
                    String transformedRecord = transformer.transform(val).orElse(val);
                    transformedRecordList.add(transformedRecord);
                }
                row.put(columnName, transformedRecordList);
            }
        }
        return row;
    }
}
