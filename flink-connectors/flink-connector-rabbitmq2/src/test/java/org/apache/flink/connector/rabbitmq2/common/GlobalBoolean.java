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
