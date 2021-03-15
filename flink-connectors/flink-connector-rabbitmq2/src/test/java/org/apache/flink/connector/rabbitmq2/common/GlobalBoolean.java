/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.connector.rabbitmq2.common;

import java.io.Serializable;

/**
 * This class holds a globally unique <code>Boolean</></code> to prevent object resets on Flink recoveries after
 * a failure has occurred. This boolean works as a singleton and keeps its state even when
 * Flink initializes a new object from a serialized checkpoint. This recovery safe behavior allows
 * us the check whether we have thrown an exception before.
 */
public class GlobalBoolean implements Serializable {
    private static boolean bool;

    /**
     * Create a new instance to access the state of the atomic boolean.
     * @param bool new state for the atomic boolean
     */
    public GlobalBoolean(boolean bool) {
        this.bool = bool;
    }

    /**
     * Sets the new state of the atomic boolean.
     * @param bool new state
     */
    public void set(boolean bool) {
        this.bool = bool;
    }

    /**
     * Returns the state of the atomic boolean.
     * @return state {true | false}
     */
    public boolean get() {
        return bool;
    }
}
