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
package org.apache.nifi.processors.debugging;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FlowDebugger extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Flowfiles processed successfully.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Flowfiles that failed to process.")
            .build();

    private static Validator percentValidator = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            Integer n = Integer.parseInt(input);
            return new ValidationResult.Builder()
                    .subject(subject)
                    .valid(0 <= n && n <= 100)
                    .input(input)
                    .explanation("must be between 0 and 100")
                    .build();
        }
    };

    public static final PropertyDescriptor SUCCESS_PERCENT = new PropertyDescriptor.Builder()
            .name("Success Percent")
            .description("Percentage of flowfiles that should be forwarded to success relationship.")
            .required(true)
            .defaultValue("50")
            .addValidator(percentValidator)
            .build();
    public static final PropertyDescriptor FAILURE_PERCENT = new PropertyDescriptor.Builder()
            .name("Failure Percent")
            .description("Percentage of flowfiles that should be forwarded to failure relationship.")
            .required(true)
            .defaultValue("15")
            .addValidator(percentValidator)
            .build();
    public static final PropertyDescriptor YIELD_PERCENT = new PropertyDescriptor.Builder()
            .name("Yield Percent")
            .description("Percentage of flowfiles that should result in the processor yielding.")
            .required(true)
            .defaultValue("15")
            .addValidator(percentValidator)
            .build();
    public static final PropertyDescriptor ROLLBACK_PERCENT = new PropertyDescriptor.Builder()
            .name("Rollback Percent")
            .description("Percentage of flowfiles that should be rolled back.")
            .required(true)
            .defaultValue("10")
            .addValidator(percentValidator)
            .build();
    public static final PropertyDescriptor PENALIZE_PERCENT = new PropertyDescriptor.Builder()
            .name("Rollback Penalize Percent")
            .description("Percentage of flowfiles that should be rolled back with penalization.")
            .required(true)
            .defaultValue("10")
            .addValidator(percentValidator)
            .build();

    private Set<Relationship> relationships = null;
    private List<PropertyDescriptor> propertyDescriptors = null;

    protected Integer SUCCESS_CUTOFF = 0;
    protected Integer FAILURE_CUTOFF = 0;
    protected Integer YIELD_CUTOFF = 0;
    protected Integer ROLLBACK_CUTOFF = 0;

    @Override
    public Set<Relationship> getRelationships() {
        if (relationships == null) {
            HashSet<Relationship> relSet = new HashSet<>();
            relSet.add(REL_SUCCESS);
            relationships = Collections.unmodifiableSet(relSet);
        }
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        if (propertyDescriptors == null) {
            ArrayList<PropertyDescriptor> propList = new ArrayList<>();
            propList.add(SUCCESS_PERCENT);
            propList.add(FAILURE_PERCENT);
            propList.add(YIELD_PERCENT);
            propList.add(ROLLBACK_PERCENT);
            propList.add(PENALIZE_PERCENT);
            propertyDescriptors = Collections.unmodifiableList(propList);
        }
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        Collection<ValidationResult> commonValidationResults = super.customValidate(context);
        if(!commonValidationResults.isEmpty()) {
            return commonValidationResults;
        }

        Collection<ValidationResult> results = new HashSet<>();
        results.add(new ValidationResult.Builder()
                .subject("Total percentages")
                .explanation("the sum of the \"Success Percent\", \"Failure Percent\", and "
                        + "\"Rollback Percent\" values must total 100.")
                .valid(context.getProperty(SUCCESS_PERCENT).asInteger()
                        + context.getProperty(FAILURE_PERCENT).asInteger()
                        + context.getProperty(YIELD_PERCENT).asInteger()
                        + context.getProperty(ROLLBACK_PERCENT).asInteger()
                        + context.getProperty(PENALIZE_PERCENT).asInteger() == 100)
                .build());
        return results;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        SUCCESS_CUTOFF = context.getProperty(SUCCESS_PERCENT).asInteger();
        FAILURE_CUTOFF = context.getProperty(FAILURE_PERCENT).asInteger() + SUCCESS_CUTOFF;
        YIELD_CUTOFF = context.getProperty(YIELD_PERCENT).asInteger() + FAILURE_CUTOFF;
        ROLLBACK_CUTOFF = context.getProperty(ROLLBACK_PERCENT).asInteger() + YIELD_CUTOFF;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile ff = session.get();
        if (ff == null) {
            return;
        }

        final ProcessorLog logger = getLogger();

        final int i = (int)(Math.random() * 100.0);
        if (i <= SUCCESS_CUTOFF) {
            logger.info("FlowDebugger transferring to success file={} UUID={}",
                    new Object[]{ff.getAttribute(CoreAttributes.FILENAME.name()),
                            ff.getAttribute(CoreAttributes.UUID.name())});
            session.transfer(ff, REL_SUCCESS);
            session.commit();
        } else if (i <= FAILURE_CUTOFF) {
            logger.info("FlowDebugger transferring to failure file={} UUID={}",
                    new Object[]{ff.getAttribute(CoreAttributes.FILENAME.name()),
                            ff.getAttribute(CoreAttributes.UUID.name())});
            session.transfer(ff, REL_FAILURE);
            session.commit();
        } else if (i <= YIELD_CUTOFF) {
            logger.info("FlowDebugger yielding file={} UUID={}",
                    new Object[]{ff.getAttribute(CoreAttributes.FILENAME.name()),
                            ff.getAttribute(CoreAttributes.UUID.name())});
            context.yield();
        } else if (i <= ROLLBACK_CUTOFF) {
            logger.info("FlowDebugger rolling back (no penalty) file={} UUID={}",
                    new Object[]{ff.getAttribute(CoreAttributes.FILENAME.name()),
                            ff.getAttribute(CoreAttributes.UUID.name())});
            session.rollback();
            session.commit();
        } else {
            logger.info("FlowDebugger rolling back (penalty) file={} UUID={}",
                    new Object[]{ff.getAttribute(CoreAttributes.FILENAME.name()),
                            ff.getAttribute(CoreAttributes.UUID.name())});
            session.rollback(true);
            session.commit();
        }
    }
}
