// package org.apache.flink.connector.rabbitmq2.source;
//
// @RunWith(SpringRunner.class)
// @SpringBootTest
// @AutoConfigureTestDatabase(replace = Replace.NONE)
// @ContextConfiguration(initializers = { RoutingTest.Initializer.class }, classes =
// RabbitMqTestConfiguration.class)
// public class ITTest {
//
//    @ClassRule
//    public static GenericContainer<?> rabbitMQContainer = new
// GenericContainer<>("rabbitmq:management")
//            .withExposedPorts(5672
//
//    @Autowired
//    private RabbitTemplate rabbitTemplate;
//
//    @Autowired
//    private NotificationRepository notificationRepository;
//
//    @Test
//    public void shouldStoreANotifcationFromTheJmsQueueAndForwardToTheRabbitMQExchange() throws
// Exception {
//
//
//    }
//
//    static class Initializer implements
// ApplicationContextInitializer<ConfigurableApplicationContext> {
//
//        @Override
//        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
//
//            DemoApplicationTestPropertyValues.using(postgreSQLContainer, activeMQContainer,
// rabbitMQContainer)
//                    .applyTo(configurableApplicationContext.getEnvironment());
//
//        }
//
//    }
//
// }
