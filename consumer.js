const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"], // Update the broker to localhost
});
const group = "test-group"
const consumer = kafka.consumer({ groupId: group });

const consumeMessages = async () => {
    await consumer.connect();
    console.log("Consumer connected!");

    await consumer.subscribe({ topic: "test-topic", fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            // console.log(`\n\nReceived message: ${message.value.toString()}`);
            console.log(heartbeat, "heartbeat")
            console.log(
                `\n\n Received: ${group}: [${topic}]: PART:${partition}:`,
                message.value.toString()
            );
        },
    });
};

consumeMessages().catch(console.error);
