package org.apache.flink.connector.rabbitmq2;

/**
 * The different consistency modes that can be defined for the sink and source individually.
 *
 * <p>The available consistency modes as follows.
 *
 * <ul>
 *   <li><code>AT_MOST_ONCE</code> Messages are consumed by the output once or never.
 *   <li><code>AT_LEAST_ONCE</code> Messages are consumed by the output at least once.
 *   <li><code>EXACTLY_ONCE</code> Messages are consumed by the output exactly once.
 * </ul>
 *
 * <p>Note that the higher the consistency guarantee gets, fewer messages can be processed by the
 * system. At-least-once and exactly-once should only be used if necessary.
 */
public enum ConsistencyMode {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE,
}
