package org.culturegraph.solr.dataimport.handler;

import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.dataimport.*;
import org.junit.Test;

import java.util.*;

import static org.culturegraph.solr.dataimport.handler.FakeMarcRecordFactory.marc21;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class SymmetricMetamorphTransformerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testDoNotTransformRowWhenInvalidRecord() {
        List fields = new ArrayList();

        Map morphField = new HashMap();
        morphField.put("column", "record");
        morphField.put(SymmetricMetamorphTransformer.MORPH_DEF, "src/test/resources/select003.xml");
        morphField.put(SymmetricMetamorphTransformer.FORMAT, "marc21");

        fields.add(morphField);

        Map row = new HashMap();
        row.put("name", "alice");
        row.put("record", "{\"id\":1}");

        VariableResolver resolver = new VariableResolver();
        resolver.addNamespace("e", row);

        Map<String, String> entityAttrs = new HashMap<>();
        entityAttrs.put("name", "e");

        Context context = getContext(null, resolver,
                null, Context.FULL_DUMP, fields, entityAttrs);

        new SymmetricMetamorphTransformer().transformRow(row, context);

        assertThat(row.get("name"), is(equalTo("alice")));
        assertThat(row.get("record"), is(equalTo("{\"id\":1}")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTransformRowWithValidRecord() {
        List fields = new ArrayList();

        Map morphField = new HashMap();
        morphField.put("column", "record");
        morphField.put(SymmetricMetamorphTransformer.MORPH_DEF, "src/test/resources/select003.xml");
        morphField.put(SymmetricMetamorphTransformer.FORMAT, "marc21");

        fields.add(morphField);

        Map row = new HashMap();
        row.put("name", "alice");
        row.put("record", marc21("001", "002", "003"));

        VariableResolver resolver = new VariableResolver();
        resolver.addNamespace("e", row);

        Map<String, String> entityAttrs = new HashMap<>();
        entityAttrs.put("name", "e");

        Context context = getContext(null, resolver,
                null, Context.FULL_DUMP, fields, entityAttrs);

        new SymmetricMetamorphTransformer().transformRow(row, context);

        assertThat(row.get("name"), is(equalTo("alice")));
        assertThat(row.get("record"), is(equalTo(marc21("003"))));
    }

    /**
     * Helper for creating a Context instance. Useful for testing Transformers
     */
    @SuppressWarnings("unchecked")
    private TestContext getContext(EntityProcessorWrapper parent,
                                   VariableResolver resolver, DataSource parentDataSource,
                                   String currProcess, final List<Map<String, String>> entityFields,
                                   final Map<String, String> entityAttrs) {
        if (resolver == null) resolver = new VariableResolver();
        final Context delegate = new ContextImpl(parent, resolver,
                parentDataSource, currProcess,
                new HashMap<>(), null, null);
        return new TestContext(entityAttrs, delegate, entityFields, parent == null);
    }

    /**
     * Helper for creating a Context instance. Useful for testing Transformers
     *
     * Copy of
     * https://github.com/apache/lucene-solr/blob/master/solr/contrib/dataimporthandler/src/test/org/apache/solr/handler/dataimport/AbstractDataImportHandlerTestCase.java#L185
     */
    @SuppressWarnings("unchecked")
    class TestContext extends Context {
        private final Map<String, String> entityAttrs;
        private final Context delegate;
        private final List<Map<String, String>> entityFields;
        private final boolean root;
        String script,scriptlang;

        public TestContext(Map<String, String> entityAttrs, Context delegate,
                           List<Map<String, String>> entityFields, boolean root) {
            this.entityAttrs = entityAttrs;
            this.delegate = delegate;
            this.entityFields = entityFields;
            this.root = root;
        }

        @Override
        public String getEntityAttribute(String name) {
            return entityAttrs == null ? delegate.getEntityAttribute(name) : entityAttrs.get(name);
        }

        @Override
        public String getResolvedEntityAttribute(String name) {
            return entityAttrs == null ? delegate.getResolvedEntityAttribute(name) :
                    delegate.getVariableResolver().replaceTokens(entityAttrs.get(name));
        }

        @Override
        public List<Map<String, String>> getAllEntityFields() {
            return entityFields == null ? delegate.getAllEntityFields()
                    : entityFields;
        }

        @Override
        public VariableResolver getVariableResolver() {
            return delegate.getVariableResolver();
        }

        @Override
        public DataSource getDataSource() {
            return delegate.getDataSource();
        }

        @Override
        public boolean isRootEntity() {
            return root;
        }

        @Override
        public String currentProcess() {
            return delegate.currentProcess();
        }

        @Override
        public Map<String, Object> getRequestParameters() {
            return delegate.getRequestParameters();
        }

        @Override
        public EntityProcessor getEntityProcessor() {
            return null;
        }

        @Override
        public void setSessionAttribute(String name, Object val, String scope) {
            delegate.setSessionAttribute(name, val, scope);
        }

        @Override
        public Object getSessionAttribute(String name, String scope) {
            return delegate.getSessionAttribute(name, scope);
        }

        @Override
        public Context getParentContext() {
            return delegate.getParentContext();
        }

        @Override
        public DataSource getDataSource(String name) {
            return delegate.getDataSource(name);
        }

        @Override
        public SolrCore getSolrCore() {
            return delegate.getSolrCore();
        }

        @Override
        public Map<String, Object> getStats() {
            return delegate.getStats();
        }


        @Override
        public String getScript() {
            return script == null ? delegate.getScript() : script;
        }

        @Override
        public String getScriptLanguage() {
            return scriptlang == null ? delegate.getScriptLanguage() : scriptlang;
        }

        @Override
        public void deleteDoc(String id) {

        }

        @Override
        public void deleteDocByQuery(String query) {

        }

        @Override
        public Object resolve(String var) {
            return delegate.resolve(var);
        }

        @Override
        public String replaceTokens(String template) {
            return delegate.replaceTokens(template);
        }
    }
}