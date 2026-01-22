/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.common.config;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.OptionTypes;

public class SmartRabbitProperties extends AbstractProperties {

    public enum Option implements IOption {
        HYBRID_EXECUTION_DIR(
                OptionTypes.STRING,
                "results/HybridExecution",
                "Directory for SmartRabbit hybrid execution output");

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        <T> Option(IOptionType<T> type, T defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public IOptionType type() {
            return type;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }

        @Override
        public String ini() {
            return "smartrabbit.hybrid_execution_dir";
        }
    }

    public SmartRabbitProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public String getHybridExecutionDir() {
        return accessor.getString(Option.HYBRID_EXECUTION_DIR);
    }
}