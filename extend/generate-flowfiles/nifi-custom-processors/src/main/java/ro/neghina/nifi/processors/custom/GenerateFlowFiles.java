/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ro.neghina.nifi.processors.custom;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"generate"})
@CapabilityDescription("This processor creates FlowFiles base on attribute")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GenerateFlowFiles extends AbstractProcessor {

    public static final PropertyDescriptor MARKETS_JSON = new PropertyDescriptor
            .Builder().name("MARKETS_JSON")
            .displayName("Markets")
            .description("JSON array with Markets attributes")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The generate FlowFile are routed to this relationship")
            .build();
    public static final Relationship REL_FAILED = new Relationship.Builder()
            .name("failed")
            .description("A failure to generate the FlowFiles base on json attribute will route the FlowFile here")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MARKETS_JSON);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String json = context.getProperty(MARKETS_JSON).evaluateAttributeExpressions().getValue();

        final List<Map<String, String>> markets = extractMarkets(json);
        if(markets.size() > 0) {
            for(Map<String, String> attributes: markets) {
                final FlowFile flowFile = createFlowFile(context, session);
                session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_SUCCESS);
            }
        }
        else {
            session.transfer(createFlowFile(context, session), REL_FAILED);
        }

        getLogger().info("Generated {} FlowFiles", new Object[] {markets.size()});
    }

    private List<Map<String, String>> extractMarkets(String markets) {
        List<Map<String, String>> response = new ArrayList<>();
        try {
            ArrayNode arrayNode = (ArrayNode) mapper.readTree(markets);
            if(arrayNode.isArray()) {
                for(JsonNode jsonNode : arrayNode) {
                    Map<String, String> obj = mapper.convertValue(jsonNode, new TypeReference<Map<String, String>>(){});
                    response.add(obj);
                }
            }
        }
        catch(Exception ex){
            ex.printStackTrace();
            getLogger().error("Failed to parse json string.");
        }
        return response;
    }

    private FlowFile createFlowFile(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.create();

        final Map<String, String> attributes = new HashMap<>();
        context.getProperties().keySet().forEach(descriptor -> {
            if (descriptor.isDynamic()) {
                final String value = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
                attributes.put(descriptor.getName(), value);
            }
        });

        if (!attributes.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }

        return flowFile;
    }

}
