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
package org.apache.hyracks.dataflow.std.util;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public final class SmartRabbitHybridExecutionDirResolver {
    public static final String CONFIG_KEY = "smartrabbit.hybrid_execution_dir";
    public static final Path DEFAULT_DIR = Paths.get("results", "HybridExecution");

    private SmartRabbitHybridExecutionDirResolver() {
    }

    /**
     * Resolves the SmartRabbit hybrid execution output directory.
     * Configure with: [common] smartrabbit.hybrid_execution_dir=/absolute/shared/path
     */
    public static Path resolve(IApplicationConfig appConfig) {
        if (appConfig == null) {
            return DEFAULT_DIR;
        }
        String configured = appConfig.getString(Section.COMMON.sectionName(), CONFIG_KEY);
        if (configured == null || configured.isEmpty()) {
            return DEFAULT_DIR;
        }
        return Paths.get(configured);
    }

    public static Path resolve(IHyracksTaskContext ctx) {
        if (ctx == null) {
            return DEFAULT_DIR;
        }
        return resolve(ctx.getJobletContext().getServiceContext().getAppConfig());
    }
}